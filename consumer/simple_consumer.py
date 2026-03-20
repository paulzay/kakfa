"""
Simple Kafka consumer.

Demonstrates:
  * Subscribing to a topic.
  * Polling for records in a loop.
  * Committing offsets (auto-commit enabled by default).
  * Graceful shutdown via KeyboardInterrupt.

Run:
    python -m consumer.simple_consumer
"""

import json
import logging
import signal
import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "simple-topic"
GROUP_ID = "simple-consumer-group"


def create_consumer(
    topic: str = TOPIC,
    group_id: str = GROUP_ID,
    bootstrap_servers: str = BOOTSTRAP_SERVERS,
    auto_offset_reset: str = "earliest",
) -> KafkaConsumer:
    """Return a KafkaConsumer subscribed to *topic*."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        # Deserialise JSON bytes back to a Python dict.
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        # Start from the earliest offset when the group has no committed offset.
        auto_offset_reset=auto_offset_reset,
        # Let Kafka commit offsets automatically every 5 s.
        enable_auto_commit=True,
        auto_commit_interval_ms=5_000,
    )
    return consumer


def consume_messages(consumer: KafkaConsumer, max_messages: int | None = None) -> list:
    """
    Poll *consumer* and return a list of decoded message values.

    If *max_messages* is given, stop after that many messages (useful for
    testing).  Otherwise run until interrupted.
    """
    received = []
    logger.info("Waiting for messages on %s …", TOPIC)

    try:
        for record in consumer:
            logger.info(
                "topic=%s  partition=%d  offset=%d  key=%s  value=%s",
                record.topic,
                record.partition,
                record.offset,
                record.key,
                record.value,
            )
            received.append(record.value)

            if max_messages is not None and len(received) >= max_messages:
                break
    except KafkaError as exc:
        logger.error("Consumer error: %s", exc)
    except KeyboardInterrupt:
        logger.info("Interrupted – shutting down.")
    finally:
        consumer.close()
        logger.info("Consumer closed.")

    return received


def main() -> None:
    # Register SIGTERM handler so `docker stop` works cleanly.
    def _handle_sigterm(*_):
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_sigterm)

    consumer = create_consumer()
    consume_messages(consumer)


if __name__ == "__main__":
    main()
