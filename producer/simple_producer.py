"""
Simple Kafka producer.

Demonstrates the most basic producer workflow:
  1. Create a KafkaProducer connected to the local broker.
  2. Serialise a plain Python dict to JSON bytes.
  3. Send it to a topic and flush/close cleanly.

Run:
    python -m producer.simple_producer
"""

import json
import logging
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "simple-topic"


def create_producer(bootstrap_servers: str = BOOTSTRAP_SERVERS) -> KafkaProducer:
    """Return a KafkaProducer that serialises values as JSON."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Wait for the leader to acknowledge the write (good default for learning).
        acks="all",
        # Retry up to 3 times on transient errors.
        retries=3,
    )


def send_message(
    producer: KafkaProducer,
    topic: str,
    message: dict,
    key: str | None = None,
) -> None:
    """Send *message* to *topic*, optionally keyed by *key*."""
    encoded_key = key.encode("utf-8") if key else None

    future = producer.send(topic, value=message, key=encoded_key)
    try:
        record_metadata = future.get(timeout=10)
        logger.info(
            "Sent to topic=%s  partition=%d  offset=%d  key=%s",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
            key,
        )
    except KafkaError as exc:
        logger.error("Failed to send message: %s", exc)
        raise


def main() -> None:
    producer = create_producer()
    logger.info("Producer connected to %s", BOOTSTRAP_SERVERS)

    messages = [
        {"id": 1, "event": "user_signup", "user": "alice"},
        {"id": 2, "event": "user_login", "user": "bob"},
        {"id": 3, "event": "purchase", "user": "alice", "amount": 49.99},
        {"id": 4, "event": "logout", "user": "bob"},
        {"id": 5, "event": "purchase", "user": "carol", "amount": 12.50},
    ]

    for msg in messages:
        send_message(producer, TOPIC, msg)
        time.sleep(0.2)

    producer.flush()
    producer.close()
    logger.info("Producer closed.")


if __name__ == "__main__":
    main()
