#!/usr/bin/env python3
"""Keycloak setup and management for GBAC demo.

Usage:
    # Set up realm, client scope, and group membership mapper
    python keycloak_setup.py setup --url http://rpdev2:8088

    # Create a client (prints client secret)
    python keycloak_setup.py create-client --url http://rpdev2:8088 --client-id alice

    # Create a group (supports nested paths)
    python keycloak_setup.py create-group --url http://rpdev2:8088 --group /mygroup

    # Delete a group
    python keycloak_setup.py delete-group --url http://rpdev2:8088 --group /mygroup

    # List all groups
    python keycloak_setup.py list-groups --url http://rpdev2:8088

    # List all clients
    python keycloak_setup.py list-clients --url http://rpdev2:8088

    # List groups a client belongs to
    python keycloak_setup.py client-groups --url http://rpdev2:8088 --client-id alice

    # List clients (service accounts) in a group
    python keycloak_setup.py group-members --url http://rpdev2:8088 --group /mygroup

    # Add a client's service account to a group (creates group if missing)
    python keycloak_setup.py add-to-group --url http://rpdev2:8088 --client-id alice --group /mygroup

    # Remove a client's service account from a group
    python keycloak_setup.py remove-from-group --url http://rpdev2:8088 --client-id alice --group /mygroup

    # Print access token for a client
    python keycloak_setup.py token --url http://rpdev2:8088 --client-id alice --client-secret <secret>

    # Print decoded (pretty-printed) access token
    python keycloak_setup.py token --url http://rpdev2:8088 --client-id alice --client-secret <secret> --decode

Requires: pip install requests
"""

import argparse
import base64
import json

import requests
import yaml


DEFAULT_URL = "http://rpdev2:8088"
REALM = "redpanda"
ADMIN_USER = "admin"
ADMIN_PASSWORD = "admin"


def get_admin_token(base_url: str) -> str:
    resp = requests.post(
        f"{base_url}/realms/master/protocol/openid-connect/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "admin-cli",
            "username": ADMIN_USER,
            "password": ADMIN_PASSWORD,
            "grant_type": "password",
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def admin_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def admin_api(base_url: str) -> str:
    return f"{base_url}/admin/realms"


# --- Setup ---


def create_realm(base_url: str, token: str) -> None:
    api = admin_api(base_url)
    resp = requests.get(f"{api}/{REALM}", headers=admin_headers(token))
    if resp.status_code == 200:
        print(f"Realm '{REALM}' already exists.")
        return

    resp = requests.post(
        api,
        headers=admin_headers(token),
        json={"realm": REALM, "enabled": True},
    )
    resp.raise_for_status()
    print(f"Created realm '{REALM}'.")


def create_client_scope(base_url: str, token: str) -> str:
    """Create the 'groups' client scope. Returns its ID."""
    api = f"{admin_api(base_url)}/{REALM}/client-scopes"
    headers = admin_headers(token)

    # Check if it already exists
    resp = requests.get(api, headers=headers)
    resp.raise_for_status()
    for scope in resp.json():
        if scope["name"] == "groups":
            print("Client scope 'groups' already exists.")
            return scope["id"]

    resp = requests.post(
        api,
        headers=headers,
        json={
            "name": "groups",
            "protocol": "openid-connect",
            "attributes": {"display.on.consent.screen": "true"},
        },
    )
    resp.raise_for_status()

    # Fetch the ID of the newly created scope
    resp = requests.get(api, headers=headers)
    resp.raise_for_status()
    for scope in resp.json():
        if scope["name"] == "groups":
            scope_id = scope["id"]
            break
    else:
        raise RuntimeError("Failed to find newly created 'groups' client scope")

    print(f"Created client scope 'groups' (id={scope_id}).")
    return scope_id


def set_scope_as_default(base_url: str, token: str, scope_id: str) -> None:
    """Register the client scope as a realm default scope."""
    api = f"{admin_api(base_url)}/{REALM}/default-default-client-scopes/{scope_id}"
    headers = admin_headers(token)
    resp = requests.put(api, headers=headers)
    resp.raise_for_status()
    print("Set 'groups' client scope as realm default.")


def create_group_mapper(base_url: str, token: str, scope_id: str) -> None:
    """Add a Group Membership protocol mapper to the client scope."""
    api = f"{admin_api(base_url)}/{REALM}/client-scopes/{scope_id}/protocol-mappers/models"
    headers = admin_headers(token)

    # Check if mapper already exists
    resp = requests.get(api, headers=headers)
    resp.raise_for_status()
    for mapper in resp.json():
        if mapper["name"] == "groups":
            print("Group membership mapper 'groups' already exists.")
            return

    resp = requests.post(
        api,
        headers=headers,
        json={
            "name": "groups",
            "protocol": "openid-connect",
            "protocolMapper": "oidc-group-membership-mapper",
            "config": {
                "claim.name": "groups",
                "full.path": "true",
                "id.token.claim": "false",
                "access.token.claim": "true",
                "userinfo.token.claim": "false",
            },
        },
    )
    resp.raise_for_status()
    print("Created group membership mapper 'groups'.")


def cmd_setup(args: argparse.Namespace) -> None:
    token = get_admin_token(args.url)
    create_realm(args.url, token)
    scope_id = create_client_scope(args.url, token)
    set_scope_as_default(args.url, token, scope_id)
    create_group_mapper(args.url, token, scope_id)
    print("\nSetup complete.")


# --- Create Client ---


def cmd_create_client(args: argparse.Namespace) -> None:
    client_id = args.client_id
    client_secret = args.client_secret

    if args.client_file:
        with open(args.client_file) as f:
            config = yaml.safe_load(f)
        client_id = client_id or config.get("client_id")
        client_secret = client_secret or config.get("client_secret")

    if not client_id:
        raise SystemExit("Error: --client-id or a client file with client_id is required")

    token = get_admin_token(args.url)
    api = f"{admin_api(args.url)}/{REALM}/clients"
    headers = admin_headers(token)

    # Check if client already exists
    resp = requests.get(api, headers=headers, params={"clientId": client_id})
    resp.raise_for_status()
    clients = [c for c in resp.json() if c["clientId"] == client_id]

    if clients:
        client_uuid = clients[0]["id"]
        print(f"Client '{client_id}' already exists (uuid={client_uuid}).")
    else:
        client_payload = {
            "clientId": client_id,
            "enabled": True,
            "clientAuthenticatorType": "client-secret",
            "serviceAccountsEnabled": True,
            "publicClient": False,
            "protocol": "openid-connect",
            "standardFlowEnabled": False,
            "directAccessGrantsEnabled": False,
        }
        if client_secret:
            client_payload["secret"] = client_secret

        resp = requests.post(api, headers=headers, json=client_payload)
        resp.raise_for_status()

        resp = requests.get(api, headers=headers, params={"clientId": client_id})
        resp.raise_for_status()
        client_uuid = resp.json()[0]["id"]
        print(f"Created client '{client_id}' (uuid={client_uuid}).")

    # Fetch the client secret
    resp = requests.get(
        f"{api}/{client_uuid}/client-secret", headers=headers
    )
    resp.raise_for_status()
    secret = resp.json()["value"]
    print(f"Client Secret: {secret}")


# --- Group Management ---


def get_client_uuid(base_url: str, token: str, client_id: str) -> str:
    api = f"{admin_api(base_url)}/{REALM}/clients"
    resp = requests.get(
        api, headers=admin_headers(token), params={"clientId": client_id}
    )
    resp.raise_for_status()
    clients = [c for c in resp.json() if c["clientId"] == client_id]
    if not clients:
        raise RuntimeError(f"Client '{client_id}' not found")
    return clients[0]["id"]


def get_service_account_user(base_url: str, token: str, client_uuid: str) -> str:
    api = f"{admin_api(base_url)}/{REALM}/clients/{client_uuid}/service-account-user"
    resp = requests.get(api, headers=admin_headers(token))
    resp.raise_for_status()
    return resp.json()["id"]


def ensure_group(base_url: str, token: str, group_path: str) -> str:  # noqa: C901
    """Ensure a group exists (creating parent groups as needed). Returns group ID."""
    api = f"{admin_api(base_url)}/{REALM}/groups"
    headers = admin_headers(token)

    parts = [p for p in group_path.strip("/").split("/") if p]
    parent_id = None

    for part in parts:
        if parent_id is None:
            resp = requests.get(api, headers=headers, params={"search": part, "exact": "true"})
        else:
            resp = requests.get(f"{api}/{parent_id}/children", headers=headers)
        resp.raise_for_status()

        found = None
        for g in resp.json():
            if g["name"] == part:
                found = g["id"]
                break

        if found:
            parent_id = found
        else:
            if parent_id is None:
                resp = requests.post(api, headers=headers, json={"name": part})
            else:
                resp = requests.post(
                    f"{api}/{parent_id}/children", headers=headers, json={"name": part}
                )
            resp.raise_for_status()

            # Re-fetch to get the ID
            if parent_id is None:
                resp = requests.get(api, headers=headers, params={"search": part, "exact": "true"})
            else:
                resp = requests.get(f"{api}/{parent_id}/children", headers=headers)
            resp.raise_for_status()
            for g in resp.json():
                if g["name"] == part:
                    parent_id = g["id"]
                    break
            else:
                raise RuntimeError(f"Failed to find group '{part}' after creation")

    if parent_id is None:
        raise ValueError(f"Invalid group path: '{group_path}'")
    return parent_id


def cmd_add_to_group(args: argparse.Namespace) -> None:
    token = get_admin_token(args.url)
    client_uuid = get_client_uuid(args.url, token, args.client_id)
    user_id = get_service_account_user(args.url, token, client_uuid)
    group_id = ensure_group(args.url, token, args.group)

    api = f"{admin_api(args.url)}/{REALM}/users/{user_id}/groups/{group_id}"
    resp = requests.put(api, headers=admin_headers(token))
    resp.raise_for_status()
    print(f"Added service account of '{args.client_id}' to group '{args.group}'.")


def cmd_remove_from_group(args: argparse.Namespace) -> None:
    token = get_admin_token(args.url)
    client_uuid = get_client_uuid(args.url, token, args.client_id)
    user_id = get_service_account_user(args.url, token, client_uuid)
    group_id = ensure_group(args.url, token, args.group)

    api = f"{admin_api(args.url)}/{REALM}/users/{user_id}/groups/{group_id}"
    resp = requests.delete(api, headers=admin_headers(token))
    resp.raise_for_status()
    print(f"Removed service account of '{args.client_id}' from group '{args.group}'.")


def cmd_create_group(args: argparse.Namespace) -> None:
    token = get_admin_token(args.url)
    group_id = ensure_group(args.url, token, args.group)
    print(f"Group '{args.group}' exists (id={group_id}).")


def find_group_by_path(base_url: str, token: str, group_path: str) -> str | None:
    """Find a group by its full path. Returns group ID or None."""
    api = f"{admin_api(base_url)}/{REALM}/groups"
    headers = admin_headers(token)

    parts = [p for p in group_path.strip("/").split("/") if p]
    parent_id = None

    for part in parts:
        if parent_id is None:
            resp = requests.get(api, headers=headers, params={"search": part, "exact": "true"})
        else:
            resp = requests.get(f"{api}/{parent_id}/children", headers=headers)
        resp.raise_for_status()

        found = None
        for g in resp.json():
            if g["name"] == part:
                found = g["id"]
                break

        if found is None:
            return None
        parent_id = found

    return parent_id


def cmd_list_groups(args: argparse.Namespace) -> None:
    token = get_admin_token(args.url)
    api = f"{admin_api(args.url)}/{REALM}/groups"
    headers = admin_headers(token)

    def fetch_and_print(groups: list, indent: int = 0) -> None:
        for g in groups:
            print(f"{'  ' * indent}{g['path']}")
            # Fetch children explicitly since top-level response may not include them
            resp = requests.get(f"{api}/{g['id']}/children", headers=headers)
            resp.raise_for_status()
            children = resp.json()
            if children:
                fetch_and_print(children, indent + 1)

    resp = requests.get(api, headers=headers)
    resp.raise_for_status()
    groups = resp.json()
    if not groups:
        print("No groups found.")
    else:
        fetch_and_print(groups)


def cmd_list_clients(args: argparse.Namespace) -> None:
    token = get_admin_token(args.url)
    api = f"{admin_api(args.url)}/{REALM}/clients"
    resp = requests.get(api, headers=admin_headers(token))
    resp.raise_for_status()

    # Filter out Keycloak built-in clients (they have no serviceAccountsEnabled or it's false)
    clients = [
        c for c in resp.json()
        if c.get("serviceAccountsEnabled") and not c["clientId"].startswith("realm-")
    ]
    if not clients:
        print("No user-created clients found.")
    else:
        for c in sorted(clients, key=lambda x: x["clientId"]):
            print(f"{c['clientId']}  (id={c['id']}, enabled={c['enabled']})")


def cmd_client_groups(args: argparse.Namespace) -> None:
    """List groups that a client's service account belongs to."""
    token = get_admin_token(args.url)
    client_uuid = get_client_uuid(args.url, token, args.client_id)
    user_id = get_service_account_user(args.url, token, client_uuid)

    api = f"{admin_api(args.url)}/{REALM}/users/{user_id}/groups"
    resp = requests.get(api, headers=admin_headers(token))
    resp.raise_for_status()

    groups = resp.json()
    if not groups:
        print(f"Client '{args.client_id}' is not a member of any groups.")
    else:
        for g in sorted(groups, key=lambda x: x["path"]):
            print(g["path"])


def cmd_group_members(args: argparse.Namespace) -> None:
    """List clients whose service accounts are members of a group."""
    token = get_admin_token(args.url)
    group_id = find_group_by_path(args.url, token, args.group)
    if group_id is None:
        print(f"Group '{args.group}' not found.")
        return

    # Get members (users) of the group
    api = f"{admin_api(args.url)}/{REALM}/groups/{group_id}/members"
    resp = requests.get(api, headers=admin_headers(token))
    resp.raise_for_status()

    members = resp.json()
    # Service account usernames follow the pattern "service-account-<client-id>"
    sa_prefix = "service-account-"
    sa_members = [m for m in members if m.get("username", "").startswith(sa_prefix)]

    if not sa_members:
        print(f"No client service accounts in group '{args.group}'.")
    else:
        for m in sorted(sa_members, key=lambda x: x["username"]):
            client_id = m["username"][len(sa_prefix):]
            print(client_id)


def cmd_delete_group(args: argparse.Namespace) -> None:
    token = get_admin_token(args.url)
    group_id = find_group_by_path(args.url, token, args.group)
    if group_id is None:
        print(f"Group '{args.group}' not found.")
        return

    api = f"{admin_api(args.url)}/{REALM}/groups/{group_id}"
    resp = requests.delete(api, headers=admin_headers(token))
    resp.raise_for_status()
    print(f"Deleted group '{args.group}'.")


# --- Token ---


def decode_jwt(token: str) -> dict:
    """Decode a JWT payload without verifying the signature."""
    payload = token.split(".")[1]
    # Add padding if needed
    padding = 4 - len(payload) % 4
    if padding != 4:
        payload += "=" * padding
    return json.loads(base64.urlsafe_b64decode(payload))


def cmd_token(args: argparse.Namespace) -> None:
    resp = requests.post(
        f"{args.url}/realms/{REALM}/protocol/openid-connect/token",
        data={
            "grant_type": "client_credentials",
            "client_id": args.client_id,
            "client_secret": args.client_secret,
        },
    )
    resp.raise_for_status()
    access_token = resp.json()["access_token"]

    if args.decode:
        print(json.dumps(decode_jwt(access_token), indent=2))
    else:
        print(access_token)


# --- CLI ---


def main() -> None:
    parser = argparse.ArgumentParser(description="Keycloak GBAC demo setup")
    parser.add_argument("--url", default=DEFAULT_URL, help="Keycloak base URL")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("setup", help="Create realm, client scope, and group mapper")

    p = sub.add_parser("create-client", help="Create a client and print its secret")
    p.add_argument("--client-id", default=None, help="Client ID (required if --client-file not given)")
    p.add_argument("--client-secret", default=None, help="Explicit client secret (optional)")
    p.add_argument("--client-file", default=None, help="YAML file with client config (client_id, client_secret, etc.)")

    p = sub.add_parser("add-to-group", help="Add client service account to a group")
    p.add_argument("--client-id", required=True)
    p.add_argument("--group", required=True, help="Group path, e.g. /mygroup")

    p = sub.add_parser("remove-from-group", help="Remove client service account from a group")
    p.add_argument("--client-id", required=True)
    p.add_argument("--group", required=True, help="Group path, e.g. /mygroup")

    sub.add_parser("list-groups", help="List all groups in the realm")
    sub.add_parser("list-clients", help="List all user-created clients in the realm")

    p = sub.add_parser("client-groups", help="List groups a client belongs to")
    p.add_argument("--client-id", required=True)

    p = sub.add_parser("group-members", help="List clients in a group")
    p.add_argument("--group", required=True, help="Group path, e.g. /mygroup")

    p = sub.add_parser("create-group", help="Create a group (with nested path support)")
    p.add_argument("--group", required=True, help="Group path, e.g. /parent/child")

    p = sub.add_parser("delete-group", help="Delete a group")
    p.add_argument("--group", required=True, help="Group path, e.g. /parent/child")

    p = sub.add_parser("token", help="Print access token for a client")
    p.add_argument("--client-id", required=True)
    p.add_argument("--client-secret", required=True)
    p.add_argument("--decode", action="store_true", help="Decode and pretty-print the JWT")

    args = parser.parse_args()
    commands = {
        "setup": cmd_setup,
        "create-client": cmd_create_client,
        "add-to-group": cmd_add_to_group,
        "remove-from-group": cmd_remove_from_group,
        "list-groups": cmd_list_groups,
        "list-clients": cmd_list_clients,
        "client-groups": cmd_client_groups,
        "group-members": cmd_group_members,
        "create-group": cmd_create_group,
        "delete-group": cmd_delete_group,
        "token": cmd_token,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
