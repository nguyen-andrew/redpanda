#!/bin/bash

docker run --rm \
  --name redpanda-console \
  --network host \
  --add-host rpdev2:127.0.1.1 \
  --entrypoint /bin/sh \
  --mount type=bind,source=$HOME/workspace/redpanda/gbac_demo/console-config.yaml,target=/tmp/config.yaml,readonly \
  -e CONFIG_FILEPATH=/tmp/config.yaml \
  docker.redpanda.com/redpandadata/console:v3.6.0 \
  -c '/app/console'