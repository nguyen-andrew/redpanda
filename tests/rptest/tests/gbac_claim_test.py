from confluent_kafka import KafkaError
from confluent_kafka.error import KafkaException
from ducktape.utils.util import wait_until

from rptest.clients.admin.proto.redpanda.core.admin.v2 import security_pb2
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.python_librdkafka import PythonLibrdkafka
from confluent_kafka.admin import (
    AclBinding,
    AclOperation,
    AclPermissionType,
    ResourcePatternType,
    ResourceType,
)
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

    def _try_produce(self, producer, topic) -> KafkaError | None:
        """Produce a message and return the delivery error, or None on success."""
        errors: list[KafkaError | None] = []
        producer.produce(
            topic, b"test", on_delivery=lambda err, _msg: errors.append(err),
        )
        producer.flush(timeout=10)
        assert len(errors) > 0, "Expected delivery callback but got none"
        return errors[0]

    def wait_until_produce_succeeds(self, producer, topic, err_msg):
        """Retry producing until it succeeds (ACL propagation may be delayed)."""
        wait_until(
            lambda: self._try_produce(producer, topic) is None,
            timeout_sec=10,
            backoff_sec=1,
            err_msg=err_msg,
        )

    def assert_produce_denied(self, producer, topic):
        """Assert that producing to the topic fails with TOPIC_AUTHORIZATION_FAILED."""
        err = self._try_produce(producer, topic)
        assert err is not None, (
            f"Expected produce to {topic} to be denied, but it succeeded"
        )
        assert err.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED, (
            f"Expected TOPIC_AUTHORIZATION_FAILED, got {err}"
        )

    def resolve_oidc_identity(self, client_id, client_secret="stub-secret"):
        """Get a token and resolve the OIDC identity via Admin API v2."""
        token = self.stub_idp.get_access_token(client_id, client_secret)
        admin_v2 = AdminV2(self.redpanda)
        req = security_pb2.ResolveOidcIdentityRequest()
        return admin_v2.security().resolve_oidc_identity(
            req,
            extra_headers={"Authorization": f"Bearer {token}"},
        )


class GbacGroupClaimFormatTest(StubOIDCTestBase):
    """Tests for various group claim formats in OIDC tokens."""

    @cluster(num_nodes=4)
    def test_json_array_groups(self):
        """Groups as JSON array ['eng', 'fin'] — each element becomes a
        group for GBAC matching."""
        client_id = "array-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "array-user",
            "groups": ["eng", "fin"],
        })

        resp = self.resolve_oidc_identity(client_id)
        assert sorted(resp.groups) == ["eng", "fin"]

        topic = "array-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer, topic, "JSON array group should grant topic access",
        )

    @cluster(num_nodes=4)
    def test_csv_groups(self):
        """Groups as CSV string 'eng,fin' — split into individual groups
        'eng' and 'fin'."""
        client_id = "csv-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "csv-user",
            "groups": "eng,fin",
        })

        resp = self.resolve_oidc_identity(client_id)
        assert sorted(resp.groups) == ["eng", "fin"]

        topic = "csv-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer, topic, "CSV group string should grant topic access",
        )

    @cluster(num_nodes=4)
    def test_csv_groups_with_whitespace(self):
        """CSV string with whitespace 'eng , fin' — whitespace is trimmed
        so groups resolve to 'eng' and 'fin'."""
        client_id = "csv-ws-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "csv-ws-user",
            "groups": "eng , fin",
        })

        resp = self.resolve_oidc_identity(client_id)
        assert sorted(resp.groups) == ["eng", "fin"]

        topic = "csv-ws-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer, topic, "CSV with whitespace should be trimmed and grant access",
        )

    @cluster(num_nodes=4)
    def test_csv_groups_with_empty_entries(self):
        """CSV string with empty entries 'eng,,,fin' — empty entries resolve
        to empty group strings, but valid groups are still matched by GBAC."""
        client_id = "csv-empty-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "csv-empty-user",
            "groups": "eng,,,fin",
        })

        expected_groups = ["", "", "eng", "fin"]
        resp = self.resolve_oidc_identity(client_id)
        assert sorted(resp.groups) == expected_groups

        topic = "csv-empty-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer, topic, "CSV with empty entries should still parse valid groups",
        )

    @cluster(num_nodes=4)
    def test_empty_array_groups(self):
        """Empty group array [] — no access granted."""
        client_id = "empty-array-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "empty-user",
            "groups": [],
        })

        resp = self.resolve_oidc_identity(client_id)
        assert len(resp.groups) == 0

        topic = "empty-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_very_long_group_name(self):
        """Very long group name (1000+ chars)."""
        long_group = "a" * 1000
        client_id = "long-name-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "long-user",
            "groups": [long_group],
        })

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == [long_group]

        topic = "long-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            f"Group:{long_group}", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer, topic, "Very long group name should work",
        )

    @cluster(num_nodes=4)
    def test_groups_as_objects(self):
        """Groups as objects [{name:x}] — should be rejected."""
        client_id = "obj-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "obj-user",
            "groups": [{"name": "eng"}],
        })

        resp = self.resolve_oidc_identity(client_id)
        assert "eng" not in resp.groups

        topic = "obj-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)


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

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == ["admin"]

        topic = "nested-path-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:admin", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer, topic, "Nested claim path should extract groups",
        )

    @cluster(num_nodes=4)
    def test_claim_path_missing(self):
        """Claim path missing — no groups, access denied."""
        client_id = "no-groups-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "no-groups-user",
        })

        resp = self.resolve_oidc_identity(client_id)
        assert len(resp.groups) == 0

        topic = "no-groups-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)


class GbacMalformedGroupClaimTest(StubOIDCTestBase):
    """Tests for invalid or malformed group claims (fail-safe behavior)."""

    @cluster(num_nodes=4)
    def test_arbitrary_string(self):
        """Groups as arbitrary string 'eng;fin'"""
        client_id = "arb-str-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "arb-str-user",
            "groups": "eng;fin",
        })

        # Not CSV (no comma), so treated as single group "eng;fin".
        resp = self.resolve_oidc_identity(client_id)
        assert "eng" not in resp.groups

        topic = "arb-str-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

        topic = "arb-str-topic2"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng;fin", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )
        self.wait_until_produce_succeeds(
            producer, topic, "Group name with semicolon should match literally",
        )

    @cluster(num_nodes=4)
    def test_groups_as_number(self):
        """Groups claim is a number — fail safe."""
        client_id = "num-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "num-user",
            "groups": 42,
        })

        resp = self.resolve_oidc_identity(client_id)
        assert len(resp.groups) == 0

        topic = "num-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:42", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_groups_as_object(self):
        """Groups claim is an object — fail safe."""
        client_id = "obj-bad-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "obj-bad-user",
            "groups": {"key": "val"},
        })

        resp = self.resolve_oidc_identity(client_id)
        assert len(resp.groups) == 0

        topic = "obj-bad-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:val", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_mixed_type_array(self):
        """Mixed-type group array ['eng', 42, null]."""
        client_id = "mixed-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "mixed-user",
            "groups": ["eng", 42, None],
        })

        resp = self.resolve_oidc_identity(client_id)
        assert len(resp.groups) == 0

        topic = "mixed-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_very_large_group_list(self):
        """Very large group list (1050 groups) — stable parsing."""
        groups = [f"g{i}" for i in range(1050)]
        client_id = "large-list-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "large-user",
            "groups": groups,
        })

        resp = self.resolve_oidc_identity(client_id)
        assert len(resp.groups) == 1050
        assert "g500" in resp.groups

        topic = "large-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:g500", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer, topic, "Large group list should still grant access for matching group",
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

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == ["Eng"]

        topic = "case-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_unicode_group_name(self):
        """Unicode group name."""
        group = "ingeniería"
        client_id = "unicode-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "unicode-user",
            "groups": [group],
        })

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == [group]

        topic = "unicode-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            f"Group:{group}", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer, topic, "Unicode group name should match correctly",
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

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == [group]

        topic = "special-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            f"Group:{group}", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer, topic, "Special character group name should match correctly",
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

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == [group]

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
        self.wait_until_produce_succeeds(
            producer, topic, "Comma in group name (array form) should be supported",
        )

    @cluster(num_nodes=4)
    def test_newline_tab_in_group_name(self):
        """Group names with newline/tab characters — parsed without crashing,
        but access is always denied because the broker rejects ACL creation
        for principals containing control characters.
        """
        nl_group = "eng\nfin"
        tab_group = "admin\tstaff"
        client_id = "ctrl-char-test"
        groups = [nl_group, tab_group]
        self.stub_idp.register_client(client_id, claims={
            "sub": "ctrl-char-user",
            "groups": groups,
        })

        resp = self.resolve_oidc_identity(client_id)
        assert sorted(resp.groups) == sorted(groups), (
            f"Expected groups {groups}, got {list(resp.groups)}"
        )

        # The broker rejects ACL creation for principals with control characters,
        # so no matching ACL can exist and access is denied. Instead, create an
        # ACL for the literal group name to verify it doesn't match the control chars
        # and grant access.
        topic = "ctrl-char-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal(
            "Group:eng", ["all"], "topic", topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_empty_string_group(self):
        """Empty string group [''] — passed through as-is by Redpanda, but access is always
        denied because the broker rejects ACL creation for empty principal names with
        INVALID_REQUEST, so no matching ACL can ever exist."""
        client_id = "empty-str-test"
        self.stub_idp.register_client(client_id, claims={
            "sub": "empty-str-user",
            "groups": [""],
        })

        resp = self.resolve_oidc_identity(client_id)
        assert resp.groups == [""]

        topic = "empty-str-topic"
        self.rpk.create_topic(topic)

        # Use the Kafka API directly to verify the broker rejects an ACL with
        # an empty principal name. Using Kafka API instead of rpk because rpk
        # silently exits 0 for this case, but the Kafka protocol returns INVALID_REQUEST.
        su_client = PythonLibrdkafka(
            self.redpanda,
            username=self.su_username,
            password=self.su_password,
            algorithm=self.su_algorithm,
        )
        admin = su_client.get_client()
        binding = AclBinding(
            restype=ResourceType.TOPIC,
            name=topic,
            resource_pattern_type=ResourcePatternType.LITERAL,
            principal="Group:",
            host="*",
            operation=AclOperation.ALL,
            permission_type=AclPermissionType.ALLOW,
        )
        results = admin.create_acls([binding])
        for _, fut in results.items():
            try:
                fut.result()
                assert False, "Expected KafkaException for empty principal name"
            except KafkaException as e:
                assert "INVALID_REQUEST" in str(e)

        # No ACL was created, so produce is denied.
        producer = self.make_producer(client_id)
        self.assert_produce_denied(producer, topic)

    @cluster(num_nodes=4)
    def test_duplicate_groups(self):
        """Duplicate groups ['admin', 'admin'] are not deduplicated."""
        client_id = "dup-test"
        groups = ["admin", "admin"]
        self.stub_idp.register_client(client_id, claims={
            "sub": "dup-user",
            "groups": groups,
        })

        resp = self.resolve_oidc_identity(client_id)
        assert list(resp.groups) == groups

        allowed_topic = "allowed-topic"
        self.rpk.create_topic(allowed_topic)
        self.rpk.sasl_allow_principal(
            "Group:admin", ["all"], "topic", allowed_topic,
            self.su_username, self.su_password, self.su_algorithm,
        )

        producer = self.make_producer(client_id)
        self.wait_until_produce_succeeds(
            producer, allowed_topic, "Produce should succeed.",
        )
