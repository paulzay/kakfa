"""
Consumer-group demo.

Demonstrates:
  * Running multiple consumers in the same group_id.
  * Kafka automatically assigns partitions so each consumer handles a share.
  * Manual offset commits for "at-least-once" processing guarantees.

Run two terminals simultaneously:
    python -m consumer.consumer_group --instance A
    python -m consumer.consumer_group --instance B

Then produce some messages and watch Kafka split the load.
"""

import argparse
import json
import logging
import signal
import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)-20s  %(message)s",
)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "advanced-topic"
GROUP_ID = "demo-consumer-group"


def create_group_consumer(
    instance_id: str,
    topic: str = TOPIC,
    group_id: str = GROUP_ID,
    bootstrap_servers: str = BOOTSTRAP_SERVERS,
) -> KafkaConsumer:
    """Return a manually-committing consumer that is part of *group_id*."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        client_id=f"{group_id}-{instance_id}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",
        # Disable auto-commit so we control exactly when an offset is saved.
        enable_auto_commit=False,
        # How long (ms) to wait for new records in a single poll call.
        max_poll_interval_ms=30_000,
    )


def run_consumer(instance_id: str, max_messages: int | None = None) -> list:
    """Start consuming and commit after each message is processed."""
    logger = logging.getLogger(f"consumer-{instance_id}")
    consumer = create_group_consumer(instance_id)
    received = []

    def _shutdown(*_):
        logger.info("Shutting down …")
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("Consumer %s started – group=%s  topic=%s", instance_id, GROUP_ID, TOPIC)

    try:
        for record in consumer:
            logger.info(
                "partition=%d  offset=%d  key=%-10s  value=%s",
                record.partition,
                record.offset,
                record.key,
                record.value,
            )
            # ---- process the message (business logic goes here) ----
            received.append(record.value)
            # ---- commit *after* successful processing ----
            consumer.commit()

            if max_messages is not None and len(received) >= max_messages:
                break
    except KafkaError as exc:
        logger.error("Consumer error: %s", exc)
    except KeyboardInterrupt:
        logger.info("Interrupted.")
    finally:
        consumer.close()
        logger.info("Consumer %s closed.", instance_id)

    return received


def main() -> None:
    parser = argparse.ArgumentParser(description="Demo consumer-group member")
    parser.add_argument(
        "--instance",
        default="A",
        help="Unique label for this consumer instance (default: A)",
    )
    args = parser.parse_args()
    run_consumer(args.instance)


if __name__ == "__main__":
    main()
