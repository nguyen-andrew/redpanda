#!/home/andrewnguyen/workspace/rp_shared_files/rp_data/gbac_demo/.venv/bin/python3
"""Kafka client for producing and consuming messages with OAUTHBEARER auth."""

import argparse
import signal
import sys
import time
import uuid

import requests
import yaml
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def fetch_token(token_endpoint: str, client_id: str, client_secret: str) -> dict:
    """Fetch an OAUTHBEARER token from Keycloak using client credentials grant."""
    resp = requests.post(
        token_endpoint,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    if resp.status_code != 200:
        print(f"Token error: {resp.status_code} {resp.text}", file=sys.stderr)
        sys.exit(1)
    return resp.json()


def oauth_cb(config: dict):
    """Return a callback for confluent-kafka to obtain an OAUTHBEARER token."""

    def _cb(_oauth_config):
        token = fetch_token(
            config["token_endpoint"], config["client_id"], config["client_secret"]
        )
        return token["access_token"], time.time() + float(token["expires_in"])

    return _cb


def make_kafka_config(config: dict, extra: dict | None = None) -> dict:
    """Build the base confluent-kafka configuration dict."""
    cfg = {
        "bootstrap.servers": config["bootstrap_servers"],
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": oauth_cb(config),
    }
    if extra:
        cfg.update(extra)
    return cfg


def handle_kafka_error(err, context: str):
    """Check for authorization errors and exit with a message."""
    code = err.code()
    if code in (
        KafkaError.TOPIC_AUTHORIZATION_FAILED,
        KafkaError.GROUP_AUTHORIZATION_FAILED,
        KafkaError.CLUSTER_AUTHORIZATION_FAILED,
    ):
        print(f"Authorization error ({context}): {err}", file=sys.stderr)
    else:
        print(f"Kafka error ({context}): {err}", file=sys.stderr)
    sys.exit(1)


def cmd_produce(args):
    config = load_config(args.client)
    producer = Producer(make_kafka_config(config))

    error_result = None

    def delivery_cb(err, msg):
        nonlocal error_result
        if err:
            error_result = err
        else:
            print(
                f"Produced to {msg.topic()}[{msg.partition()}]@{msg.offset()}: "
                f"{msg.value().decode()}"
            )

    for message in args.message:
        kwargs = {}
        if args.key:
            kwargs["key"] = args.key.encode()
        if args.headers:
            kwargs["headers"] = dict(h.split("=", 1) for h in args.headers)
        producer.produce(
            args.topic,
            value=message.encode(),
            callback=delivery_cb,
            **kwargs,
        )

    producer.flush(timeout=30)

    if error_result is not None:
        handle_kafka_error(error_result, "produce")


def cmd_consume(args):
    config = load_config(args.client)

    extra = {
        "group.id": args.group
        or config.get("client_id", f"demo-{uuid.uuid4().hex[:8]}"),
        "auto.offset.reset": args.offset,
        "session.timeout.ms": 6000,
        "max.poll.interval.ms": 10000,
    }
    consumer = Consumer(make_kafka_config(config, extra))
    consumer.subscribe(args.topic)

    shutdown = False

    def sighandler(_signum, _frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGINT, sighandler)
    signal.signal(signal.SIGTERM, sighandler)

    count = 0
    try:
        while not shutdown:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            err = msg.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    print(
                        f"--- End of {msg.topic()}[{msg.partition()}] "
                        f"at offset {msg.offset()} ---"
                    )
                    continue
                handle_kafka_error(err, "consume")

            raw_key = msg.key()
            raw_value = msg.value()
            key = raw_key.decode() if raw_key is not None else None
            value = raw_value.decode() if raw_value is not None else None
            headers = msg.headers() or []
            header_str = ""
            if headers:
                header_str = (
                    " headers=["
                    + ", ".join(f"{k}={v.decode() if v else ''}" for k, v in headers)
                    + "]"
                )

            print(
                f"{msg.topic()}[{msg.partition()}]@{msg.offset()}: "
                f"key={key} value={value}{header_str}"
            )

            count += 1
            if args.count and count >= args.count:
                break
    except KafkaException as e:
        handle_kafka_error(e.args[0], "consume")
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True, help="Path to client config YAML")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # produce subcommand
    p_produce = subparsers.add_parser("produce", help="Produce messages")
    p_produce.add_argument("--topic", required=True, help="Topic to produce to")
    p_produce.add_argument(
        "--message",
        "-m",
        required=True,
        nargs="+",
        help="Message(s) to produce (repeat for multiple)",
    )
    p_produce.add_argument("--key", "-k", help="Message key")
    p_produce.add_argument(
        "--headers",
        "-H",
        nargs="+",
        metavar="KEY=VALUE",
        help="Message headers (key=value pairs)",
    )

    # consume subcommand
    p_consume = subparsers.add_parser("consume", help="Consume messages")
    p_consume.add_argument(
        "--topic", "-t", required=True, nargs="+", help="Topic(s) to consume from"
    )
    p_consume.add_argument(
        "--group",
        "-g",
        default=None,
        help="Consumer group ID (default: random)",
    )
    p_consume.add_argument(
        "--offset",
        default="earliest",
        choices=["earliest", "latest"],
        help="Auto offset reset (default: earliest)",
    )
    p_consume.add_argument(
        "--count", "-c", type=int, help="Exit after consuming N messages"
    )

    args = parser.parse_args()

    if args.command == "produce":
        cmd_produce(args)
    elif args.command == "consume":
        cmd_consume(args)


if __name__ == "__main__":
    main()
