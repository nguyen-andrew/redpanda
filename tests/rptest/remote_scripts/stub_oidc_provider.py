from __future__ import annotations

import base64
import http.server
import json
import signal
import socketserver
import time
from types import FrameType
from typing import Any
from urllib.parse import parse_qs

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.rsa import (
    RSAPrivateKey,
    RSAPublicKey,
    generate_private_key,
)
import jwt as pyjwt


SUPPORTED_ALGS = ("RS256", "RS384", "RS512", "ES256", "ES384", "ES512")
EC_CURVES = {"ES256": ec.SECP256R1, "ES384": ec.SECP384R1, "ES512": ec.SECP521R1}
AUDIENCE = "redpanda"


def base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def generate_rsa_keypair() -> tuple[RSAPrivateKey, RSAPublicKey]:
    private_key = generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    return private_key, public_key


def public_key_to_jwk(
    public_key: RSAPublicKey, kid: str, alg: str = "RS256"
) -> dict[str, str]:
    public_numbers = public_key.public_numbers()
    n_bytes = public_numbers.n.to_bytes((public_numbers.n.bit_length() + 7) // 8, "big")
    e_bytes = public_numbers.e.to_bytes((public_numbers.e.bit_length() + 7) // 8, "big")
    return {
        "kty": "RSA",
        "alg": alg,
        "use": "sig",
        "kid": kid,
        "n": base64url_encode(n_bytes),
        "e": base64url_encode(e_bytes),
    }


def ec_key_to_jwk(
    public_key: ec.EllipticCurvePublicKey, alg: str, kid: str
) -> dict[str, str]:
    nums = public_key.public_numbers()
    crv = {"ES256": "P-256", "ES384": "P-384", "ES512": "P-521"}[alg]
    # RFC 7518 6.2.1.2: coordinates MUST be full field width, never
    # bit_length-derived (that flakes ~1/128 keys on a leading zero byte).
    width = (public_key.curve.key_size + 7) // 8
    return {
        "kty": "EC",
        "alg": alg,
        "use": "sig",
        "kid": kid,
        "crv": crv,
        "x": base64url_encode(nums.x.to_bytes(width, "big")),
        "y": base64url_encode(nums.y.to_bytes(width, "big")),
    }


def generate_keys() -> dict[str, dict[str, Any]]:
    """Mint one signing key per supported algorithm."""
    keys: dict[str, dict[str, Any]] = {}
    for alg in SUPPORTED_ALGS:
        kid = f"stub-key-{alg.lower()}"
        if alg in EC_CURVES:
            priv = ec.generate_private_key(EC_CURVES[alg]())
            keys[alg] = {
                "kid": kid,
                "private": priv,
                "jwk": ec_key_to_jwk(priv.public_key(), alg, kid),
            }
        else:
            priv = generate_rsa_keypair()[0]
            keys[alg] = {
                "kid": kid,
                "private": priv,
                "jwk": public_key_to_jwk(priv.public_key(), kid, alg=alg),
            }
    return keys


class BaseHandler(http.server.BaseHTTPRequestHandler):
    def log_request(self, *args: Any, **kwargs: Any) -> None:
        return

    def json_log(self, response_code: int) -> None:
        log_item = {
            "path": self.path,
            "method": self.command,
            "response_code": response_code,
        }
        print(json.dumps(log_item), flush=True)

    def send_json(self, data: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.json_log(status)


def make_handler(
    keys: dict[str, dict[str, Any]],
    issuer: str,
    token_lifetime: int,
) -> type[BaseHandler]:
    jwks_doc: dict[str, list[dict[str, str]]] = {
        "keys": [entry["jwk"] for entry in keys.values()]
    }
    clients: dict[str, dict[str, Any]] = {}

    class OIDCHandler(BaseHandler):
        def do_GET(self) -> None:
            if self.path == "/.well-known/openid-configuration":
                self.send_json(
                    {
                        "issuer": issuer,
                        "jwks_uri": f"{issuer}/jwks",
                        "token_endpoint": f"{issuer}/token",
                    }
                )
            elif self.path == "/jwks":
                self.send_json(jwks_doc)
            else:
                self.send_response(404)
                self.end_headers()
                self.json_log(404)

        def do_POST(self) -> None:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")

            if self.path == "/register":
                data = json.loads(body)
                client_id = data["client_id"]
                alg = data.get("alg", "RS256")
                if alg not in SUPPORTED_ALGS:
                    self.send_json({"error": "unsupported_alg"}, status=400)
                    return
                clients[client_id] = {
                    "secret": data.get("client_secret", "stub-secret"),
                    "claims": data.get("claims", {}),
                    "alg": alg,
                }
                self.send_json({"status": "registered", "client_id": client_id})

            elif self.path == "/token":
                params = parse_qs(body)
                client_id = params.get("client_id", [None])[0]

                if client_id is None or client_id not in clients:
                    self.send_json({"error": "invalid_client"}, status=400)
                    return

                now = time.time()
                payload = {
                    "iss": issuer,
                    "aud": AUDIENCE,
                    "iat": int(now),
                    "exp": int(now) + token_lifetime,
                }
                # Merge registered claims as-is — no validation
                payload.update(clients[client_id]["claims"])

                entry = keys[clients[client_id]["alg"]]
                token = pyjwt.encode(
                    payload,
                    entry["private"],
                    algorithm=clients[client_id]["alg"],
                    headers={"kid": entry["kid"]},
                )

                self.send_json(
                    {
                        "access_token": token,
                        "token_type": "bearer",
                        "expires_in": token_lifetime,
                    }
                )
            else:
                self.send_response(404)
                self.end_headers()
                self.json_log(404)

    return OIDCHandler


def main() -> None:
    import argparse
    import socket

    parser = argparse.ArgumentParser(description="Stub OIDC Provider")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--issuer", type=str, default=None)
    parser.add_argument("--token-lifetime", type=int, default=3600)
    options = parser.parse_args()

    if options.issuer is None:
        hostname = socket.getfqdn()
        options.issuer = f"http://{hostname}:{options.port}"

    keys = generate_keys()
    handler = make_handler(keys, options.issuer, options.token_lifetime)

    class ReuseAddressTcpServer(socketserver.TCPServer):
        allow_reuse_address = True

    with ReuseAddressTcpServer(("", options.port), handler) as httpd:

        def _stop(_signum: int, _frame: FrameType | None) -> None:
            httpd.server_close()
            exit(0)

        signal.signal(signal.SIGTERM, _stop)
        print(
            json.dumps(
                {"status": "ready", "port": options.port, "issuer": options.issuer}
            ),
            flush=True,
        )
        httpd.serve_forever()


if __name__ == "__main__":
    main()
