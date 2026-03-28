#!/bin/bash

docker compose -f ~/workspace/redpanda/gbac_demo/docker-compose.yaml down; docker compose -f ~/workspace/redpanda/gbac_demo/docker-compose.yaml up -d

# Wait for Keycloak to be ready (up to 60s)
echo "Waiting for Keycloak to start up..."
for i in $(seq 1 30); do
    if curl -sf http://rpdev2:8088/realms/master > /dev/null 2>&1; then
        echo "Keycloak is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: Keycloak did not become ready in time."
        exit 1
    fi
    sleep 2
done

# Set up realm, client scope, and group membership mapper
~/workspace/redpanda/gbac_demo/keycloak_setup.py setup

~/workspace/redpanda/gbac_demo/keycloak_setup.py create-client --client-file ~/workspace/redpanda/gbac_demo/clients/alice.yaml

~/workspace/redpanda/gbac_demo/keycloak_setup.py create-client --client-file ~/workspace/redpanda/gbac_demo/clients/bob.yaml

~/workspace/redpanda/gbac_demo/keycloak_setup.py create-group --group /sales

~/workspace/redpanda/gbac_demo/keycloak_setup.py create-group --group /analytics

# Start off with just alice in the sales team.
~/workspace/redpanda/gbac_demo/keycloak_setup.py add-to-group --client-id alice --group /sales