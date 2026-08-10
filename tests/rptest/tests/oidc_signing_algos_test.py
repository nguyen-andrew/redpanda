from __future__ import annotations

import requests
from ducktape.mark import matrix

from rptest.services.cluster import cluster
from rptest.tests.gbac_claim_test import StubOIDCTestBase


SUPPORTED_ALGS = ["RS256", "RS384", "RS512", "ES256", "ES384", "ES512"]


class OIDCSigningAlgosTest(StubOIDCTestBase):
    """Broker accepts OAUTHBEARER tokens for every supported JWT signing
    algorithm, against a mixed multi-algorithm JWKS."""

    @cluster(num_nodes=4)
    @matrix(alg=SUPPORTED_ALGS)
    def test_signing_alg(self, alg: str) -> None:
        client_id = f"client-{alg.lower()}"
        sub = f"sub-{alg.lower()}"
        self.stub_idp.register_client(client_id, {"sub": sub}, alg=alg)

        # The stub mints one key per supported alg, so this is a real mixed
        # keyset, not a single-key JWKS.
        jwks = requests.get(f"{self.stub_idp.url}/jwks", timeout=5).json()
        kids = {k["kid"] for k in jwks["keys"]}
        expected = {f"stub-key-{a.lower()}" for a in SUPPORTED_ALGS}
        assert kids == expected, jwks

        topic = "algos-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            f"User:{sub}",
            ["all"],
            "topic",
            topic,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )
        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(producer, topic, f"produce failed for {alg}")
