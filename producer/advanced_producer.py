"""
Advanced Kafka producer.

Demonstrates:
  * Message keys   – control which partition a message lands in.
  * Partitioning   – verify that same-key messages go to the same partition.
  * Custom headers – attach metadata without touching the payload.
  * Callbacks      – on_send_success / on_send_error for async confirmation.
  * Batching       – linger_ms and batch_size tuning.

Run:
    python -m producer.advanced_producer
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
TOPIC = "advanced-topic"


def on_send_success(record_metadata) -> None:
    logger.info(
        "[callback] topic=%s  partition=%d  offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(exc: Exception) -> None:
    logger.error("[callback] send failed: %s", exc)


def create_advanced_producer(
    bootstrap_servers: str = BOOTSTRAP_SERVERS,
) -> KafkaProducer:
    """Return a KafkaProducer tuned for batching and async callbacks."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        # Accumulate messages for up to 10 ms before sending a batch.
        linger_ms=10,
        # Maximum bytes in a single batch (16 KB).
        batch_size=16_384,
        # Compress messages to reduce network traffic.
        compression_type="gzip",
    )


def send_with_key(producer: KafkaProducer, topic: str, key: str, value: dict) -> None:
    """Send *value* keyed by *key* using async callbacks."""
    (
        producer.send(topic, key=key, value=value)
        .add_callback(on_send_success)
        .add_errback(on_send_error)
    )


def send_with_headers(
    producer: KafkaProducer,
    topic: str,
    value: dict,
    headers: list[tuple[str, bytes]],
) -> None:
    """Send *value* with custom *headers* (list of (name, bytes) tuples)."""
    future = producer.send(topic, value=value, headers=headers)
    try:
        meta = future.get(timeout=10)
        logger.info(
            "Sent with headers to partition=%d offset=%d",
            meta.partition,
            meta.offset,
        )
    except KafkaError as exc:
        logger.error("Failed to send message with headers: %s", exc)
        raise


def main() -> None:
    producer = create_advanced_producer()
    logger.info("Advanced producer connected to %s", BOOTSTRAP_SERVERS)

    # --- keyed messages ---
    # Messages with the same key always land in the same partition,
    # guaranteeing ordering per key.
    users = ["alice", "bob", "carol"]
    events = ["login", "purchase", "logout", "login"]

    logger.info("--- Sending keyed messages ---")
    for i, event in enumerate(events):
        user = users[i % len(users)]
        send_with_key(
            producer,
            TOPIC,
            key=user,
            value={"user": user, "event": event, "seq": i},
        )

    # Wait for async sends to complete before moving on.
    producer.flush()

    # --- headers ---
    logger.info("--- Sending message with custom headers ---")
    send_with_headers(
        producer,
        TOPIC,
        value={"order_id": 42, "total": 99.99},
        headers=[
            ("source", b"checkout-service"),
            ("version", b"2"),
        ],
    )

    producer.flush()
    producer.close()
    logger.info("Advanced producer closed.")


if __name__ == "__main__":
    main()
