#!/bin/bash

cd ~/workspace/redpanda/gbac_demo

# SALES TEAM SETUP
# Set up a direct group ACL to allow the /sales group to produce & consume with the sales-topic
rpk --config ~/workspace/rp_shared_files/rp_data/rpk.yaml security acl create --allow-principal Group:/sales --operation all --topic sales-topic --group '*'

# ANALYTICS TEAM SETUP
# Set up a direct group ACL to allow the /analytics group to produce & consume with the analytics-topic
rpk --config ~/workspace/rp_shared_files/rp_data/rpk.yaml security acl create --allow-principal Group:/analytics --operation all --topic analytics-topic --group '*'

# read-only role
rpk --config ~/workspace/rp_shared_files/rp_data/rpk.yaml security role create topic-reader
# This grants `READ` on all topics (to fetch messages) and all consumer groups (to join a group). Without `WRITE`, the role can't produce.
rpk --config ~/workspace/rp_shared_files/rp_data/rpk.yaml security acl create --allow-role topic-reader --operation read --topic '*' --group '*'
# Assign the topic-reader role to the /analytics group
rpk --config ~/workspace/rp_shared_files/rp_data/rpk.yaml security role assign topic-reader --group /analytics