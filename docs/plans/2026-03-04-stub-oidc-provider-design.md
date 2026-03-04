# Stub OIDC Provider for GBAC Integration Tests

## Problem

The GBAC test matrix (Confluence: GBAC Test cases, results and automation info)
identifies ~30 test scenarios in sections B-E that lack CI coverage. These test
group claim path resolution, claim format handling (CSV strings, malformed
claims, mixed-type arrays), and group name edge cases (unicode, special
characters, duplicates, case sensitivity).

Keycloak, the only IdP in the integration test suite, always emits groups as a
JSON array at a fixed claim path. It cannot produce the claim structures needed
for these tests (CSV strings, objects, numbers, nested paths, etc.).

## Solution

A lightweight **Stub OIDC Provider** that gives tests full control over JWT
claim structure. It implements just enough of the OIDC spec for Redpanda to
validate tokens, while letting tests dictate exactly what claims appear in
each JWT — including malformed or non-standard formats.

## Architecture

```
Test Code (coordinator)                Remote Node
┌───────────────────────┐          ┌─────────────────────────┐
│  StubOIDCProvider     │          │  stub_oidc_provider.py  │
│  (ducktape service)   │──HTTP──▶ │  (http.server + PyJWT)  │
│                       │          │                         │
│  register_client(     │  POST    │  /register              │
│    client_id,         │ ──────▶  │  stores claims template │
│    claims={...})      │          │                         │
│                       │          │  /token                 │
│  generate_oauth_config│          │  signs JWT with stored  │
│    → OAuthConfig      │          │  claims for client_id   │
└───────────────────────┘          │                         │
                                   │  /.well-known/...       │
   Redpanda ◄──────────────────── │  discovery + /jwks      │
   (fetches JWKS, validates JWTs) └─────────────────────────┘
```

## Components

### 1. Remote Script: `stub_oidc_provider.py`

Location: `tests/rptest/remote_scripts/stub_oidc_provider.py`

A standalone Python HTTP server using `http.server` + `PyJWT` + `cryptography`
(available via `python-keycloak` transitive dependencies). Generates an RSA key
pair at startup.

#### Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/.well-known/openid-configuration` | GET | Discovery doc: `{"issuer": "...", "jwks_uri": "...", "token_endpoint": "..."}` |
| `/jwks` | GET | JWKS with the stub's RSA-2048 public key |
| `/token` | POST | Accepts `client_credentials` grant, returns JWT signed with claims registered for that `client_id` |
| `/register` | POST | Control endpoint: registers a `client_id` with a claims template |

#### Token Issuance Logic

On `POST /token`:
1. Parse `client_id` from the form-encoded body
2. Look up the registered claims template for that `client_id`
3. Build JWT payload:
   - Set `iss` to the stub's issuer URL
   - Set `aud` to `"redpanda"` (hardcoded, matches Redpanda config)
   - Set `iat` to current time, `exp` to current time + configured lifetime
   - Merge the registered claims template **as-is** (no validation or
     transformation — this is the key design property)
4. Sign with RS256 using the stub's private key
5. Return standard OAuth2 response: `{"access_token": "...", "expires_in": N, "token_type": "bearer"}`

The "as-is" merge means tests can inject any claim structure:
- `{"groups": "eng,fin"}` — CSV string
- `{"groups": [{"name": "x"}]}` — objects in array
- `{"groups": 42}` — number
- `{"realm_access": {"groups": ["admin"]}}` — nested path
- `{"groups": []}` — empty array

The stub does not validate claims. It signs whatever it is told to.

#### JWT Requirements (from Redpanda's validation)

Redpanda only supports RS256. The JWT must have:
- Header: `{"alg": "RS256", "typ": "JWT", "kid": "<matching kid>"}`
- `iss`: must exactly match the `issuer` from the discovery document
- `aud`: must contain the configured `oidc_token_audience` (hardcoded `"redpanda"`)
- `exp`: unix timestamp in the future (within 30s clock skew)
- `sub` (or configured principal claim): non-empty string

#### Startup Arguments

- `--port` (default: 8090)
- `--issuer` (default: `http://<hostname>:<port>`)
- `--token-lifetime` (default: 3600 seconds)

### 2. Ducktape Service: `StubOIDCProvider`

Location: `tests/rptest/services/stub_oidc_provider.py`

Inherits from `HttpServer`/`BackgroundThreadService`, following the
`MockIamRolesServer` pattern. Deploys and manages the remote script.

#### Public API

```python
class StubOIDCProvider(HttpServer):
    def __init__(self, context, port=8090):
        ...

    def register_client(self, client_id, claims, client_secret="stub-secret"):
        """Register a client with specific JWT claims.

        claims is a dict merged into the JWT payload as-is.
        Standard claims (iss, aud, iat, exp) are set by the stub
        automatically and should not be included unless testing
        override behavior.
        """

    def generate_oauth_config(self, node, client_id, client_secret="stub-secret"):
        """Return an OAuthConfig pointing at the stub's token endpoint.

        Compatible with PythonLibrdkafka — no client-side changes needed.
        """

    def get_discovery_url(self, node):
        """Return the OIDC discovery URL for Redpanda's oidc_discovery_url config."""
```

These methods mirror `KeycloakService`/`KeycloakAdminClient`:
- `register_client()` replaces `create_client()` + `create_group_mapper()` + `add_service_user_to_group()`
- `generate_oauth_config()` and `get_discovery_url()` work identically

### 3. Test Base Class: `StubOIDCTestBase`

Location: in `tests/rptest/tests/redpanda_oauth_test.py` (or a new file)

Mirrors `RedpandaOIDCTestBase` but uses `StubOIDCProvider`:

```python
class StubOIDCTestBase(RedpandaTest):
    def __init__(self, test_context, **kwargs):
        self.stub_idp = StubOIDCProvider(test_context)

        security = SecurityConfig()
        security.enable_sasl = True
        security.sasl_mechanisms = ["SCRAM", "OAUTHBEARER"]
        security.http_authentication = ["BASIC", "OIDC"]

        super().__init__(
            test_context,
            security=security,
            extra_rp_conf={
                "oidc_discovery_url": ...,  # set in setUp after nodes allocated
                "oidc_token_audience": "redpanda",
            },
            **kwargs,
        )
```

### 4. Test Cases

Each test follows the same pattern — only the `claims` dict changes:

```python
class StubOIDCGroupClaimTest(StubOIDCTestBase):
    @cluster(num_nodes=4)  # 3 redpanda + 1 stub
    def test_csv_groups(self):
        """GBAC-GRP-FMT-020: Groups as CSV string"""
        self.stub_idp.register_client("csv-test", claims={
            "sub": "csv-user",
            "groups": "eng,fin",
        })
        topic = "csv-topic"
        self.rpk.create_topic(topic)
        self.rpk.sasl_allow_principal("Group:eng", ["describe", "read"], "topic", topic)

        cfg = self.stub_idp.generate_oauth_config(self.stub_node, "csv-test")
        client = PythonLibrdkafka(self.redpanda, oauth_config=cfg)
        assert topic in client.get_visible_topics()
```

## Target Test Scenarios (Sections B-E)

### B. Group Claim Path Resolution
| Test ID | Scenario | Claims |
|---------|----------|--------|
| GBAC-GRP-PATH-020 | Nested path `$.claims.groups` | `{"claims": {"groups": ["admin"]}}` + config `oidc_group_claim_path: "$.claims.groups"` |
| GBAC-GRP-PATH-030 | Claim path missing | `{"sub": "user"}` (no groups at all) |
| GBAC-GRP-PATH-040 | Path resolves to object | `{"groups": {"name": "admin"}}` |
| GBAC-GRP-PATH-050 | Path resolves to number | `{"groups": 42}` |

### C. Group Claim Format Handling
| Test ID | Scenario | Claims |
|---------|----------|--------|
| GBAC-GRP-FMT-010 | JSON array | `{"groups": ["eng", "fin"]}` |
| GBAC-GRP-FMT-020 | CSV string | `{"groups": "eng,fin"}` |
| GBAC-GRP-FMT-030 | CSV with whitespace | `{"groups": "eng , fin"}` |
| GBAC-GRP-FMT-040 | CSV with empty entries | `{"groups": "eng,,fin"}` |
| GBAC-GRP-FMT-050 | Empty array | `{"groups": []}` |
| GBAC-GRP-FMT-060 | Very long group name | `{"groups": ["a" * 1000]}` |
| GBAC-GRP-FMT-070 | Groups as objects | `{"groups": [{"name": "x"}]}` |

### D. Invalid / Malformed Group Claims
| Test ID | Scenario | Claims |
|---------|----------|--------|
| GBAC-GRP-BAD-010 | Arbitrary string | `{"groups": "eng;fin"}` |
| GBAC-GRP-BAD-020 | Number | `{"groups": 42}` |
| GBAC-GRP-BAD-030 | Object | `{"groups": {"key": "val"}}` |
| GBAC-GRP-BAD-040 | Mixed-type array | `{"groups": ["eng", 42, null]}` |
| GBAC-GRP-BAD-050 | Very large list (1000+) | `{"groups": [f"g{i}" for i in range(1050)]}` |

### E. Group Name Edge Cases
| Test ID | Scenario | Claims |
|---------|----------|--------|
| GBAC-GRP-NAME-010 | Case mismatch | `{"groups": ["Eng"]}` with ACL for `Group:eng` |
| GBAC-GRP-NAME-020 | Unicode | `{"groups": ["ingeniería"]}` |
| GBAC-GRP-NAME-030 | Special characters | `{"groups": ["eng@dev#1"]}` |
| GBAC-GRP-NAME-040 | Comma in name (array) | `{"groups": ["eng,fin"]}` |
| GBAC-GRP-NAME-050 | Comma in name (CSV) | `{"groups": "\"eng,fin\""}` |
| GBAC-GRP-NAME-060 | Empty string | `{"groups": [""]}` |
| GBAC-GRP-NAME-070 | Duplicates | `{"groups": ["admin", "admin"]}` |
| GBAC-GRP-NAME-080 | Newline/tab chars | `{"groups": ["eng\ndev"]}` |

## Dependencies

All already available in the test environment:
- `http.server`, `socketserver`, `json`, `signal` (stdlib)
- `PyJWT` + `cryptography` (transitive via `python-keycloak==5.8.1`)

No new dependencies required.

## Files to Create/Modify

| File | Action |
|------|--------|
| `tests/rptest/remote_scripts/stub_oidc_provider.py` | Create — remote HTTP server script |
| `tests/rptest/services/stub_oidc_provider.py` | Create — ducktape service wrapper |
| `tests/rptest/tests/gbac_claim_test.py` | Create — test cases for sections B-E |

No modifications to existing files needed. `PythonLibrdkafka`, `OAuthConfig`,
and the Redpanda OIDC config all work as-is.
