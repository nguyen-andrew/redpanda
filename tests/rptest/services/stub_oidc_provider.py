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

    def get_access_token(self, client_id, client_secret="stub-secret"):
        """Request an access token for a registered client."""
        resp = requests.post(
            f"{self.url}/token",
            data={"client_id": client_id, "client_secret": client_secret},
            timeout=5,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

    def get_discovery_url(self, node):
        return f"http://{node.account.hostname}:{self.port}/.well-known/openid-configuration"
