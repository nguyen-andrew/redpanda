# Stub OIDC Provider Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a stub OIDC provider ducktape service that lets integration tests inject arbitrary JWT claim structures, enabling CI coverage for GBAC test scenarios B-E.

**Architecture:** A remote Python HTTP script (`stub_oidc_provider.py`) generates an RSA key pair at startup and serves OIDC discovery, JWKS, token, and registration endpoints. A ducktape service wrapper (`StubOIDCProvider`) manages the remote script lifecycle and exposes `register_client()` / `generate_oauth_config()` / `get_discovery_url()` methods mirroring `KeycloakService`. Tests use a `StubOIDCTestBase` base class and follow the same `PythonLibrdkafka` + `OAuthConfig` pattern as existing OIDC tests.

**Tech Stack:** Python stdlib `http.server`/`socketserver`, `PyJWT` + `cryptography` (transitive deps of `python-keycloak`), ducktape `BackgroundThreadService`

---

## Task 1: Remote Script — Key Generation and JWKS Endpoint

**Files:**
- Create: `tests/rptest/remote_scripts/stub_oidc_provider.py`

**Step 1: Write the remote script skeleton with RSA key gen and JWKS**

Create the file with imports, key generation, and the `/jwks` GET handler. Follow the pattern in `tests/rptest/remote_scripts/aws_iam_role_mock.py`.

```python
import base64
import http.server
import json
import signal
import socketserver
import time
from urllib.parse import parse_qs

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import jwt as pyjwt


KID = "stub-key-1"
AUDIENCE = "redpanda"


def base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def generate_rsa_keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    return private_key, public_key


def public_key_to_jwk(public_key, kid: str) -> dict:
    public_numbers = public_key.public_numbers()
    n_bytes = public_numbers.n.to_bytes((public_numbers.n.bit_length() + 7) // 8, "big")
    e_bytes = public_numbers.e.to_bytes((public_numbers.e.bit_length() + 7) // 8, "big")
    return {
        "kty": "RSA",
        "alg": "RS256",
        "use": "sig",
        "kid": kid,
        "n": base64url_encode(n_bytes),
        "e": base64url_encode(e_bytes),
    }


class BaseHandler(http.server.BaseHTTPRequestHandler):
    def log_request(self, *args, **kwargs):
        return

    def json_log(self, response_code):
        log_item = {
            "path": self.path,
            "method": self.command,
            "response_code": response_code,
        }
        print(json.dumps(log_item), flush=True)

    def send_json(self, data, status=200):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.json_log(status)


def make_handler(private_key, public_key, issuer, token_lifetime):
    jwk = public_key_to_jwk(public_key, KID)
    jwks_doc = {"keys": [jwk]}
    clients = {}  # client_id -> {"secret": str, "claims": dict}

    class OIDCHandler(BaseHandler):
        def do_GET(self):
            if self.path == "/.well-known/openid-configuration":
                self.send_json({
                    "issuer": issuer,
                    "jwks_uri": f"{issuer}/jwks",
                    "token_endpoint": f"{issuer}/token",
                })
            elif self.path == "/jwks":
                self.send_json(jwks_doc)
            else:
                self.send_response(404)
                self.end_headers()
                self.json_log(404)

        def do_POST(self):
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode("utf-8")

            if self.path == "/register":
                data = json.loads(body)
                client_id = data["client_id"]
                clients[client_id] = {
                    "secret": data.get("client_secret", "stub-secret"),
                    "claims": data.get("claims", {}),
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

                token = pyjwt.encode(
                    payload,
                    private_key,
                    algorithm="RS256",
                    headers={"kid": KID},
                )

                self.send_json({
                    "access_token": token,
                    "token_type": "bearer",
                    "expires_in": token_lifetime,
                })
            else:
                self.send_response(404)
                self.end_headers()
                self.json_log(404)

    return OIDCHandler


def main():
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

    private_key, public_key = generate_rsa_keypair()
    handler = make_handler(private_key, public_key, options.issuer, options.token_lifetime)

    class ReuseAddressTcpServer(socketserver.TCPServer):
        allow_reuse_address = True

    with ReuseAddressTcpServer(("", options.port), handler) as httpd:
        def _stop(*_):
            httpd.server_close()
            exit(0)

        signal.signal(signal.SIGTERM, _stop)
        print(json.dumps({"status": "ready", "port": options.port, "issuer": options.issuer}), flush=True)
        httpd.serve_forever()


if __name__ == "__main__":
    main()
```

**Step 2: Verify the script runs standalone**

Run on local machine to confirm key gen + endpoints work:

```bash
cd tests/rptest/remote_scripts
python3 stub_oidc_provider.py --port 8090 &
# In another terminal:
curl http://localhost:8090/.well-known/openid-configuration
curl http://localhost:8090/jwks
curl -X POST http://localhost:8090/register -d '{"client_id":"test","claims":{"sub":"user1","groups":["eng"]}}'
curl -X POST http://localhost:8090/token -d 'client_id=test&grant_type=client_credentials'
kill %1
```

Expected: discovery doc returns JSON with issuer/jwks_uri/token_endpoint, jwks returns RSA key, register returns `{"status":"registered"}`, token returns `{"access_token":"eyJ...","token_type":"bearer","expires_in":3600}`.

**Step 3: Commit**

```bash
git add tests/rptest/remote_scripts/stub_oidc_provider.py
git commit -m "testing: add stub OIDC provider remote script

Standalone HTTP server that generates an RSA key pair at startup and
serves OIDC discovery, JWKS, client registration, and token endpoints.
Tests register clients with arbitrary JWT claims templates; the stub
signs whatever it is told to, enabling tests for non-standard group
claim formats that real IdPs cannot produce."
```

---

## Task 2: Ducktape Service Wrapper

**Files:**
- Create: `tests/rptest/services/stub_oidc_provider.py`
- Reference: `tests/rptest/services/mock_iam_roles_server.py` (pattern to follow)
- Reference: `tests/rptest/services/http_server.py` (base class)
- Reference: `tests/rptest/services/keycloak.py` (for `OAuthConfig` import)

**Step 1: Write the ducktape service**

Follow `MockIamRolesServer` pattern exactly. The service deploys the remote script, waits for the `{"status": "ready"}` log line, then exposes `register_client()`, `generate_oauth_config()`, and `get_discovery_url()`.

```python
import json
import os

import requests
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until

from rptest.services.http_server import HttpServer
from rptest.services.keycloak import OAuthConfig
from rptest.util import inject_remote_script


SCRIPT_NAME = "stub_oidc_provider.py"


class StubOIDCProvider(HttpServer):
    LOG_DIR = "/tmp/stub_oidc_provider"
    STDOUT_CAPTURE = os.path.join(LOG_DIR, "stub_oidc_provider.stdout")

    logs = {
        "stub_oidc_provider_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": True,
        },
    }

    def __init__(self, context, port=8090, token_lifetime=3600):
        super(HttpServer, self).__init__(context, 1)
        self.port = port
        self.token_lifetime = token_lifetime
        self.stop_timeout_sec = 5
        self.requests = []
        self.hostname = self.nodes[0].account.hostname
        self.address = f"{self.hostname}:{self.port}"
        self.url = f"http://{self.address}"
        self.remote_script_path = None

    def _worker(self, idx, node):
        node.account.ssh(f"mkdir -p {self.LOG_DIR}", allow_fail=False)
        self.remote_script_path = inject_remote_script(node, SCRIPT_NAME)
        cmd = (
            f"python3 -u {self.remote_script_path} "
            f"--port {self.port} "
            f"--token-lifetime {self.token_lifetime} 2>&1"
        )
        cmd += f" | tee -a {self.STDOUT_CAPTURE} &"

        self.logger.debug(f"Starting stub OIDC provider {self.url}")
        for line in node.account.ssh_capture(cmd):
            self.logger.debug(f"stub_oidc: {line}")
            parsed = self.try_parse_json(node, line.strip())
            if parsed is not None:
                if "response_code" in parsed:
                    self.requests.append(parsed)

    def pids(self, node):
        try:
            cmd = f"ps ax | grep {SCRIPT_NAME} | grep -v grep | awk '{{print $1}}'"
            pid_arr = [
                pid
                for pid in node.account.ssh_capture(
                    cmd, allow_fail=True, callback=int
                )
            ]
            return pid_arr
        except (RemoteCommandError, ValueError):
            return []

    def wait_ready(self, timeout_sec=30):
        wait_until(
            lambda: self._is_ready(),
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg="Stub OIDC provider did not become ready",
        )

    def _is_ready(self):
        try:
            resp = requests.get(
                f"{self.url}/.well-known/openid-configuration", timeout=2
            )
            return resp.status_code == 200
        except Exception:
            return False

    def register_client(self, client_id, claims, client_secret="stub-secret"):
        resp = requests.post(
            f"{self.url}/register",
            json={
                "client_id": client_id,
                "client_secret": client_secret,
                "claims": claims,
            },
            timeout=5,
        )
        resp.raise_for_status()
        return resp.json()

    def generate_oauth_config(self, node, client_id, client_secret="stub-secret"):
        return OAuthConfig(
            client_id=client_id,
            client_secret=client_secret,
            token_endpoint=f"http://{node.account.hostname}:{self.port}/token",
        )

    def get_discovery_url(self, node):
        return f"http://{node.account.hostname}:{self.port}/.well-known/openid-configuration"
```

**Step 2: Commit**

```bash
git add tests/rptest/services/stub_oidc_provider.py
git commit -m "testing: add StubOIDCProvider ducktape service wrapper

Manages the stub_oidc_provider.py remote script lifecycle following the
MockIamRolesServer pattern. Exposes register_client(), generate_oauth_config(),
and get_discovery_url() that mirror KeycloakService's API, making it a
drop-in replacement for tests that need custom JWT claim structures."
```

---

## Task 3: Test Base Class and First Smoke Test

**Files:**
- Create: `tests/rptest/tests/gbac_claim_test.py`
- Reference: `tests/rptest/tests/redpanda_oauth_test.py:96-189` (`RedpandaOIDCTestBase` pattern)
- Reference: `tests/rptest/services/keycloak.py` (for `OAuthConfig`)

**Step 1: Write the test base class and a smoke test**

The smoke test (GBAC-GRP-FMT-010) uses a standard JSON array — the simplest case — to prove the infrastructure works end-to-end before testing exotic formats.

```python
import time

from ducktape.utils.util import wait_until

from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    LoggingConfig,
    PandaproxyConfig,
    SchemaRegistryConfig,
    SecurityConfig,
    make_redpanda_service,
)
from rptest.services.stub_oidc_provider import StubOIDCProvider
from rptest.tests.redpanda_oauth_test import NestedGroupType

from ducktape.tests.test import Test


log_config = LoggingConfig(
    "info",
    logger_levels={
        "security": "trace",
        "kafka/client": "trace",
        "kafka": "debug",
    },
)


class StubOIDCTestBase(Test):
    """Base class for GBAC tests using StubOIDCProvider instead of Keycloak."""

    def __init__(self, test_context, num_nodes=4, **kwargs):
        super().__init__(test_context, **kwargs)
        num_brokers = num_nodes - 1
        self.stub_idp = StubOIDCProvider(test_context)

        security = SecurityConfig()
        security.enable_sasl = True
        security.sasl_mechanisms = ["SCRAM", "OAUTHBEARER"]
        security.http_authentication = ["BASIC", "OIDC"]

        stub_node = self.stub_idp.nodes[0]

        self.redpanda = make_redpanda_service(
            test_context,
            num_brokers,
            extra_rp_conf={
                "oidc_discovery_url": self.stub_idp.get_discovery_url(stub_node),
                "oidc_token_audience": "redpanda",
            },
            security=security,
            log_config=log_config,
        )

        self.su_username, self.su_password, self.su_algorithm = (
            self.redpanda.SUPERUSER_CREDENTIALS
        )

        self.rpk = RpkTool(
            self.redpanda,
            username=self.su_username,
            password=self.su_password,
            sasl_mechanism=self.su_algorithm,
        )

    def setUp(self):
        # Start stub IdP first so Redpanda can fetch JWKS on startup
        self.stub_idp.start()
        self.stub_idp.wait_ready()
        self.redpanda.start()

    def get_visible_topics(self, client_id, client_secret="stub-secret"):
        """Create a fresh OIDC client and return visible topics."""
        stub_node = self.stub_idp.nodes[0]
        cfg = self.stub_idp.generate_oauth_config(stub_node, client_id, client_secret)
        k_client = PythonLibrdkafka(
            self.redpanda,
            algorithm="OAUTHBEARER",
            oauth_config=cfg,
        )
        producer = k_client.get_producer()
        producer.poll(0.0)
        return set(producer.list_topics(timeout=5).topics.keys())


class GbacGroupClaimFormatTest(StubOIDCTestBase):
    """GBAC Section C: Group claim format handling tests."""

    @cluster(num_nodes=4)
    def test_json_array_groups(self):
        """GBAC-GRP-FMT-010: Groups as JSON array — smoke test for stub infra."""
        client_id = "array-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "array-user",
            "groups": ["eng", "fin"],
        })

        topic = "array-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        wait_until(
            lambda: topic in self.get_visible_topics(client_id),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="JSON array group should grant topic access",
        )
```

**Step 2: Run the test to verify it passes**

```bash
# From the repo root, using ducktape (exact invocation depends on your CI setup):
ducktape tests/rptest/tests/gbac_claim_test.py::GbacGroupClaimFormatTest.test_json_array_groups
```

Expected: PASS — this validates the entire stub OIDC provider infrastructure works end-to-end.

**Step 3: Commit**

```bash
git add tests/rptest/tests/gbac_claim_test.py
git commit -m "testing/gbac: add StubOIDCTestBase and JSON array smoke test

Introduces the test base class for GBAC claim tests using
StubOIDCProvider, and a smoke test (GBAC-GRP-FMT-010) that validates
the full end-to-end flow: stub key gen, JWKS serving, token issuance,
and Redpanda OIDC validation with group-based authorization."
```

---

## Task 4: Section C — Group Claim Format Tests

**Files:**
- Modify: `tests/rptest/tests/gbac_claim_test.py`

**Step 1: Add remaining Section C tests to `GbacGroupClaimFormatTest`**

Add these test methods after `test_json_array_groups`:

```python
    @cluster(num_nodes=4)
    def test_csv_groups(self):
        """GBAC-GRP-FMT-020: Groups as CSV string 'eng,fin'."""
        client_id = "csv-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "csv-user",
            "groups": "eng,fin",
        })

        topic = "csv-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        wait_until(
            lambda: topic in self.get_visible_topics(client_id),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="CSV group string should grant topic access",
        )

    @cluster(num_nodes=4)
    def test_csv_groups_with_whitespace(self):
        """GBAC-GRP-FMT-030: CSV string with whitespace 'eng , fin'."""
        client_id = "csv-ws-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "csv-ws-user",
            "groups": "eng , fin",
        })

        topic = "csv-ws-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        wait_until(
            lambda: topic in self.get_visible_topics(client_id),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="CSV with whitespace should be trimmed and grant access",
        )

    @cluster(num_nodes=4)
    def test_csv_groups_with_empty_entries(self):
        """GBAC-GRP-FMT-040: CSV string with empty entries 'eng,,fin'."""
        client_id = "csv-empty-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "csv-empty-user",
            "groups": "eng,,fin",
        })

        topic = "csv-empty-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        wait_until(
            lambda: topic in self.get_visible_topics(client_id),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="CSV with empty entries should still parse valid groups",
        )

    @cluster(num_nodes=4)
    def test_empty_array_groups(self):
        """GBAC-GRP-FMT-050: Empty group array [] — no access granted."""
        client_id = "empty-array-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "empty-user",
            "groups": [],
        })

        topic = "empty-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        # Wait to let ACLs propagate, then verify no access
        time.sleep(3)
        visible = self.get_visible_topics(client_id)
        assert topic not in visible, (
            f"Empty group array should grant no access, but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_very_long_group_name(self):
        """GBAC-GRP-FMT-060: Very long group name (1000+ chars)."""
        long_group = "a" * 1000
        client_id = "long-name-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "long-user",
            "groups": [long_group],
        })

        topic = "long-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            f"Group:{long_group}", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        wait_until(
            lambda: topic in self.get_visible_topics(client_id),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Very long group name should work",
        )

    @cluster(num_nodes=4)
    def test_groups_as_objects(self):
        """GBAC-GRP-FMT-070: Groups as objects [{name:x}] — should be rejected."""
        client_id = "obj-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "obj-user",
            "groups": [{"name": "eng"}],
        })

        topic = "obj-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        time.sleep(3)
        visible = self.get_visible_topics(client_id)
        assert topic not in visible, (
            f"Object groups should be rejected, but {topic} was visible"
        )
```

**Step 2: Run all Section C tests**

```bash
ducktape tests/rptest/tests/gbac_claim_test.py::GbacGroupClaimFormatTest
```

Expected: All pass.

**Step 3: Commit**

```bash
git add tests/rptest/tests/gbac_claim_test.py
git commit -m "testing/gbac: add Section C group claim format tests

Covers GBAC-GRP-FMT-020 through 070: CSV strings, CSV with whitespace,
CSV with empty entries, empty array, very long group name, and groups
as objects. All use the stub OIDC provider for full claim control."
```

---

## Task 5: Section B — Group Claim Path Resolution Tests

**Files:**
- Modify: `tests/rptest/tests/gbac_claim_test.py`

**Step 1: Add a new test class for Section B**

These tests need to change `oidc_group_claim_path` at runtime via `set_cluster_config`.

```python
class GbacGroupClaimPathTest(StubOIDCTestBase):
    """GBAC Section B: Group claim path resolution tests."""

    @cluster(num_nodes=4)
    def test_nested_claim_path(self):
        """GBAC-GRP-PATH-020: Nested path $.realm_access.groups."""
        self.redpanda.set_cluster_config({
            "oidc_group_claim_path": "$.realm_access.groups",
        })

        client_id = "nested-path-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "nested-user",
            "realm_access": {"groups": ["admin"]},
        })

        topic = "nested-path-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:admin", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        wait_until(
            lambda: topic in self.get_visible_topics(client_id),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Nested claim path should extract groups",
        )

    @cluster(num_nodes=4)
    def test_claim_path_missing(self):
        """GBAC-GRP-PATH-030: Claim path missing — no groups, access denied."""
        client_id = "no-groups-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "no-groups-user",
            # No groups claim at all
        })

        topic = "no-groups-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        time.sleep(3)
        visible = self.get_visible_topics(client_id)
        assert topic not in visible, (
            f"Missing groups claim should deny access, but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_claim_path_resolves_to_object(self):
        """GBAC-GRP-PATH-040: Claim path resolves to object — fail safe, denied."""
        client_id = "obj-path-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "obj-path-user",
            "groups": {"name": "admin"},
        })

        topic = "obj-path-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:admin", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        time.sleep(3)
        visible = self.get_visible_topics(client_id)
        assert topic not in visible, (
            f"Object at group path should deny access, but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_claim_path_resolves_to_number(self):
        """GBAC-GRP-PATH-050: Claim path resolves to number — fail safe, denied."""
        client_id = "num-path-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "num-path-user",
            "groups": 42,
        })

        topic = "num-path-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:42", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        time.sleep(3)
        visible = self.get_visible_topics(client_id)
        assert topic not in visible, (
            f"Number at group path should deny access, but {topic} was visible"
        )
```

**Step 2: Run Section B tests**

```bash
ducktape tests/rptest/tests/gbac_claim_test.py::GbacGroupClaimPathTest
```

**Step 3: Commit**

```bash
git add tests/rptest/tests/gbac_claim_test.py
git commit -m "testing/gbac: add Section B group claim path resolution tests

Covers GBAC-GRP-PATH-020 through 050: nested claim path, missing claim
path, path resolving to object, and path resolving to number."
```

---

## Task 6: Section D — Invalid / Malformed Group Claims Tests

**Files:**
- Modify: `tests/rptest/tests/gbac_claim_test.py`

**Step 1: Add a new test class for Section D**

```python
class GbacMalformedGroupClaimTest(StubOIDCTestBase):
    """GBAC Section D: Invalid / malformed group claims (fail-safe)."""

    @cluster(num_nodes=4)
    def test_arbitrary_string(self):
        """GBAC-GRP-BAD-010: Groups as arbitrary string 'eng;fin' — fail safe."""
        client_id = "arb-str-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "arb-str-user",
            "groups": "eng;fin",
        })

        topic = "arb-str-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        time.sleep(3)
        visible = self.get_visible_topics(client_id)
        assert topic not in visible, (
            f"Arbitrary string 'eng;fin' should not match Group:eng, but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_groups_as_number(self):
        """GBAC-GRP-BAD-020: Groups claim is a number — fail safe."""
        client_id = "num-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "num-user",
            "groups": 42,
        })

        topic = "num-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:42", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        time.sleep(3)
        visible = self.get_visible_topics(client_id)
        assert topic not in visible, (
            f"Number groups claim should deny access, but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_groups_as_object(self):
        """GBAC-GRP-BAD-030: Groups claim is an object — fail safe."""
        client_id = "obj-bad-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "obj-bad-user",
            "groups": {"key": "val"},
        })

        topic = "obj-bad-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:val", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        time.sleep(3)
        visible = self.get_visible_topics(client_id)
        assert topic not in visible, (
            f"Object groups claim should deny access, but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_mixed_type_array(self):
        """GBAC-GRP-BAD-040: Mixed-type group array ['eng', 42, null]."""
        client_id = "mixed-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "mixed-user",
            "groups": ["eng", 42, None],
        })

        topic = "mixed-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        # Behavior is TBD per the test matrix — either valid strings extracted
        # or entire claim rejected. Test that it doesn't crash.
        time.sleep(3)
        # Just verify no crash — access may or may not be granted
        self.get_visible_topics(client_id)

    @cluster(num_nodes=4)
    def test_very_large_group_list(self):
        """GBAC-GRP-BAD-050: Very large group list (1050 groups) — stable parsing."""
        groups = [f"g{i}" for i in range(1050)]
        client_id = "large-list-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "large-user",
            "groups": groups,
        })

        topic = "large-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:g500", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        wait_until(
            lambda: topic in self.get_visible_topics(client_id),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Large group list should still grant access for matching group",
        )
```

**Step 2: Run Section D tests**

```bash
ducktape tests/rptest/tests/gbac_claim_test.py::GbacMalformedGroupClaimTest
```

**Step 3: Commit**

```bash
git add tests/rptest/tests/gbac_claim_test.py
git commit -m "testing/gbac: add Section D malformed group claim tests

Covers GBAC-GRP-BAD-010 through 050: arbitrary string, number, object,
mixed-type array, and very large group list (1050 entries)."
```

---

## Task 7: Section E — Group Name Edge Cases Tests

**Files:**
- Modify: `tests/rptest/tests/gbac_claim_test.py`

**Step 1: Add a new test class for Section E**

```python
class GbacGroupNameEdgeCaseTest(StubOIDCTestBase):
    """GBAC Section E: Group name edge cases."""

    @cluster(num_nodes=4)
    def test_case_mismatch(self):
        """GBAC-GRP-NAME-010: Case mismatch 'Eng' vs ACL 'eng' — exact match only."""
        client_id = "case-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "case-user",
            "groups": ["Eng"],
        })

        topic = "case-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        time.sleep(3)
        visible = self.get_visible_topics(client_id)
        assert topic not in visible, (
            f"Case mismatch should deny access (exact match only), but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_unicode_group_name(self):
        """GBAC-GRP-NAME-020: Unicode group name."""
        group = "ingeniería"
        client_id = "unicode-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "unicode-user",
            "groups": [group],
        })

        topic = "unicode-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            f"Group:{group}", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        wait_until(
            lambda: topic in self.get_visible_topics(client_id),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Unicode group name should match correctly",
        )

    @cluster(num_nodes=4)
    def test_special_characters(self):
        """GBAC-GRP-NAME-030: Special characters in group name."""
        group = "eng@dev#1"
        client_id = "special-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "special-user",
            "groups": [group],
        })

        topic = "special-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            f"Group:{group}", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        wait_until(
            lambda: topic in self.get_visible_topics(client_id),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Special character group name should match correctly",
        )

    @cluster(num_nodes=4)
    def test_comma_in_group_name_array(self):
        """GBAC-GRP-NAME-040: Group name with comma in JSON array form."""
        group = "eng,fin"
        client_id = "comma-array-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "comma-array-user",
            "groups": [group],  # Array form — comma is part of the name
        })

        topic = "comma-array-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            f"Group:{group}", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        wait_until(
            lambda: topic in self.get_visible_topics(client_id),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Comma in group name (array form) should be supported",
        )

    @cluster(num_nodes=4)
    def test_empty_string_group(self):
        """GBAC-GRP-NAME-060: Empty string group [''] — ignored or denied."""
        client_id = "empty-str-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "empty-str-user",
            "groups": [""],
        })

        topic = "empty-str-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        time.sleep(3)
        visible = self.get_visible_topics(client_id)
        assert topic not in visible, (
            f"Empty string group should not grant access, but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_duplicate_groups(self):
        """GBAC-GRP-NAME-070: Duplicate groups ['admin', 'admin'] — deduplicated."""
        client_id = "dup-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "dup-user",
            "groups": ["admin", "admin"],
        })

        topic = "dup-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:admin", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        wait_until(
            lambda: topic in self.get_visible_topics(client_id),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Duplicate groups should be deduplicated and grant access",
        )
```

Note: GBAC-GRP-NAME-050 (comma in CSV form) and GBAC-GRP-NAME-080 (newline/tab) are omitted since the test matrix notes these are edge cases with caveats (can't create ACLs for newline group names via rpk). Add them later if needed.

**Step 2: Run Section E tests**

```bash
ducktape tests/rptest/tests/gbac_claim_test.py::GbacGroupNameEdgeCaseTest
```

**Step 3: Commit**

```bash
git add tests/rptest/tests/gbac_claim_test.py
git commit -m "testing/gbac: add Section E group name edge case tests

Covers GBAC-GRP-NAME-010 through 070: case mismatch, unicode, special
characters, comma in name, empty string group, and duplicate groups."
```

---

## Task 8: Final Review and Cleanup

**Step 1: Run all GBAC claim tests together**

```bash
ducktape tests/rptest/tests/gbac_claim_test.py
```

Expected: All tests pass.

**Step 2: Run linting**

```bash
cd tests && python -m py_compile rptest/tests/gbac_claim_test.py
cd tests && python -m py_compile rptest/services/stub_oidc_provider.py
cd tests && python -m py_compile rptest/remote_scripts/stub_oidc_provider.py
```

**Step 3: Final commit if any fixups needed**

```bash
git add -u
git commit -m "testing/gbac: lint and fixup cleanup"
```
