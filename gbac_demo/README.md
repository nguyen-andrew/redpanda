Run all of these commands from the base `redpanda` directory.

```bash
# Start keycloak container & set up the clients & groups.
./gbac_demo/setup_keycloak.sh

# Start a fresh redpanda instance.
./gbac_demo/clear-local-rp.sh && ./gbac_demo/start-rp-local.sh

# Configure permissions setup for demo.
./gbac_demo/setup_rpk.sh
```

You can consume and/or produce as `alice` or `bob`.

```bash
# Produce will produce one message to the topic and return.
./gbac_demo/demo_kclient.py --client ~/workspace/redpanda/gbac_demo/clients/alice.yaml produce --topic sales-topic --message 'Alice: Hello sales!'
```

```bash
# Consume will read from the earliest and continue to read until you interrupt it.
./gbac_demo/demo_kclient.py --client ~/workspace/redpanda/gbac_demo/clients/alice.yaml consume --topic sales-topic
```

```bash
# Bob won't be able to produce to this topic until you add him to the `/analytics` group in Keycloak.
./gbac_demo/demo_kclient.py --client ~/workspace/redpanda/gbac_demo/clients/bob.yaml produce --topic analytics-topic --message 'Bob: Hello analytics!'
```

```bash
# Bob won't be able to consume from this topic until you add him to the `/analytics` group in Keycloak.
./gbac_demo/demo_kclient.py --client ~/workspace/redpanda/gbac_demo/clients/bob.yaml consume --topic analytics-topic
```

To move Bob to the `/analytics` group: 
1. Go to http://rpdev2:8088
2. Sign in with `admin:admin`
3. Go to the `redpanda` realm (Manage realms -> `redpanda`)
4. Clients -> Click `bob` in the Clients list -> Click "Service accounts roles" -> Click `service-account-bob`
5. Click "Groups" -> Click "Join Group"
6. Select `analytics` and click "Join".

You can manage his groups (and Alice's groups) in that way.