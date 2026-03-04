import time

from ducktape.utils.util import wait_until

from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    LoggingConfig,
    SecurityConfig,
    make_redpanda_service,
)
from rptest.services.stub_oidc_provider import StubOIDCProvider

from ducktape.tests.test import Test

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
            log_config=LoggingConfig(
                "info",
                logger_levels={
                    "security": "trace",
                    "kafka/client": "trace",
                    "kafka": "debug",
                },
            ),
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
        self.stub_idp.start()
        self.stub_idp.wait_ready()
        self.redpanda.start()

    def make_producer(self, client_id, client_secret="stub-secret"):
        """Create a Kafka producer authenticated via the stub OIDC provider."""
        stub_node = self.stub_idp.nodes[0]
        cfg = self.stub_idp.generate_oauth_config(stub_node, client_id, client_secret)
        k_client = PythonLibrdkafka(
            self.redpanda,
            algorithm="OAUTHBEARER",
            oauth_config=cfg,
        )
        producer = k_client.get_producer()
        producer.poll(0.0)
        return producer

    def get_visible_topics(self, producer):
        """Return the set of topics visible to the given producer."""
        return set(producer.list_topics(timeout=5).topics.keys())


class GbacGroupClaimFormatTest(StubOIDCTestBase):
    """Tests for various group claim formats in OIDC tokens."""

    @cluster(num_nodes=4)
    def test_json_array_groups(self):
        """Groups as JSON array — smoke test for stub infra."""
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

        producer = self.make_producer(client_id)
        wait_until(
            lambda: topic in self.get_visible_topics(producer),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="JSON array group should grant topic access",
        )

    @cluster(num_nodes=4)
    def test_csv_groups(self):
        """Groups as CSV string 'eng,fin'."""
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

        producer = self.make_producer(client_id)
        wait_until(
            lambda: topic in self.get_visible_topics(producer),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="CSV group string should grant topic access",
        )

    @cluster(num_nodes=4)
    def test_csv_groups_with_whitespace(self):
        """CSV string with whitespace 'eng , fin'."""
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

        producer = self.make_producer(client_id)
        wait_until(
            lambda: topic in self.get_visible_topics(producer),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="CSV with whitespace should be trimmed and grant access",
        )

    @cluster(num_nodes=4)
    def test_csv_groups_with_empty_entries(self):
        """CSV string with empty entries 'eng,,fin'."""
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

        producer = self.make_producer(client_id)
        wait_until(
            lambda: topic in self.get_visible_topics(producer),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="CSV with empty entries should still parse valid groups",
        )

    @cluster(num_nodes=4)
    def test_empty_array_groups(self):
        """Empty group array [] — no access granted."""
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

        producer = self.make_producer(client_id)
        time.sleep(3)
        visible = self.get_visible_topics(producer)
        assert topic not in visible, (
            f"Empty group array should grant no access, but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_very_long_group_name(self):
        """Very long group name (1000+ chars)."""
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

        producer = self.make_producer(client_id)
        wait_until(
            lambda: topic in self.get_visible_topics(producer),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Very long group name should work",
        )

    @cluster(num_nodes=4)
    def test_groups_as_objects(self):
        """Groups as objects [{name:x}] — should be rejected."""
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

        producer = self.make_producer(client_id)
        time.sleep(3)
        visible = self.get_visible_topics(producer)
        assert topic not in visible, (
            f"Object groups should be rejected, but {topic} was visible"
        )


class GbacGroupClaimPathTest(StubOIDCTestBase):
    """Tests for group claim path resolution with various oidc_group_claim_path configs."""

    @cluster(num_nodes=4)
    def test_nested_claim_path(self):
        """Nested path $.realm_access.groups."""
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

        producer = self.make_producer(client_id)
        wait_until(
            lambda: topic in self.get_visible_topics(producer),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Nested claim path should extract groups",
        )

    @cluster(num_nodes=4)
    def test_claim_path_missing(self):
        """Claim path missing — no groups, access denied."""
        client_id = "no-groups-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "no-groups-user",
        })

        topic = "no-groups-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        time.sleep(3)
        visible = self.get_visible_topics(producer)
        assert topic not in visible, (
            f"Missing groups claim should deny access, but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_claim_path_resolves_to_object(self):
        """Claim path resolves to object — fail safe, denied."""
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

        producer = self.make_producer(client_id)
        time.sleep(3)
        visible = self.get_visible_topics(producer)
        assert topic not in visible, (
            f"Object at group path should deny access, but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_claim_path_resolves_to_number(self):
        """Claim path resolves to number — fail safe, denied."""
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

        producer = self.make_producer(client_id)
        time.sleep(3)
        visible = self.get_visible_topics(producer)
        assert topic not in visible, (
            f"Number at group path should deny access, but {topic} was visible"
        )


class GbacMalformedGroupClaimTest(StubOIDCTestBase):
    """Tests for invalid or malformed group claims (fail-safe behavior)."""

    @cluster(num_nodes=4)
    def test_arbitrary_string(self):
        """Groups as arbitrary string 'eng;fin' — fail safe."""
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

        producer = self.make_producer(client_id)
        time.sleep(3)
        visible = self.get_visible_topics(producer)
        assert topic not in visible, (
            f"Arbitrary string 'eng;fin' should not match Group:eng, but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_groups_as_number(self):
        """Groups claim is a number — fail safe."""
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

        producer = self.make_producer(client_id)
        time.sleep(3)
        visible = self.get_visible_topics(producer)
        assert topic not in visible, (
            f"Number groups claim should deny access, but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_groups_as_object(self):
        """Groups claim is an object — fail safe."""
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

        producer = self.make_producer(client_id)
        time.sleep(3)
        visible = self.get_visible_topics(producer)
        assert topic not in visible, (
            f"Object groups claim should deny access, but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_mixed_type_array(self):
        """Mixed-type group array ['eng', 42, null]."""
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

        # Either valid strings are extracted or the entire claim is rejected.
        # Test that it doesn't crash.
        producer = self.make_producer(client_id)
        time.sleep(3)
        self.get_visible_topics(producer)

    @cluster(num_nodes=4)
    def test_very_large_group_list(self):
        """Very large group list (1050 groups) — stable parsing."""
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

        producer = self.make_producer(client_id)
        wait_until(
            lambda: topic in self.get_visible_topics(producer),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Large group list should still grant access for matching group",
        )


class GbacGroupNameEdgeCaseTest(StubOIDCTestBase):
    """Tests for group name edge cases (case, unicode, special chars, etc)."""

    @cluster(num_nodes=4)
    def test_case_mismatch(self):
        """Case mismatch 'Eng' vs ACL 'eng' — exact match only."""
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

        producer = self.make_producer(client_id)
        time.sleep(3)
        visible = self.get_visible_topics(producer)
        assert topic not in visible, (
            f"Case mismatch should deny access (exact match only), but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_unicode_group_name(self):
        """Unicode group name."""
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

        producer = self.make_producer(client_id)
        wait_until(
            lambda: topic in self.get_visible_topics(producer),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Unicode group name should match correctly",
        )

    @cluster(num_nodes=4)
    def test_special_characters(self):
        """Special characters in group name."""
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

        producer = self.make_producer(client_id)
        wait_until(
            lambda: topic in self.get_visible_topics(producer),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Special character group name should match correctly",
        )

    @cluster(num_nodes=4)
    def test_comma_in_group_name_array(self):
        """Group name with comma in JSON array form."""
        group = "eng,fin"
        client_id = "comma-array-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "comma-array-user",
            "groups": [group],
        })

        topic = "comma-array-topic"
        self.rpk.create_topic(topic)
        # rpk's --allow-principal flag treats commas as delimiters between
        # multiple principals. Without quoting, "Group:eng,fin" would be
        # split into two ACL entries: Group:eng and User:fin (the second
        # defaulting to User type since it lacks a prefix). This is
        # rpk-specific behavior — the underlying Kafka protocol handles
        # commas in principal names without issue. CSV-style double quotes
        # prevent rpk from splitting on the comma.
        self.rpk.sasl_allow_principal(
            f'"Group:{group}"', ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        wait_until(
            lambda: topic in self.get_visible_topics(producer),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Comma in group name (array form) should be supported",
        )

    @cluster(num_nodes=4)
    def test_empty_string_group(self):
        """Empty string group [''] — ignored or denied."""
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

        producer = self.make_producer(client_id)
        time.sleep(3)
        visible = self.get_visible_topics(producer)
        assert topic not in visible, (
            f"Empty string group should not grant access, but {topic} was visible"
        )

    @cluster(num_nodes=4)
    def test_duplicate_groups(self):
        """Duplicate groups ['admin', 'admin'] — deduplicated."""
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

        producer = self.make_producer(client_id)
        wait_until(
            lambda: topic in self.get_visible_topics(producer),
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Duplicate groups should be deduplicated and grant access",
        )
