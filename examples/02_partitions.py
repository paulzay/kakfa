"""
Example 02 – Partitions & Message Keys

Kafka splits a topic into *partitions*, which allows it to scale horizontally.
When you provide a message *key*, Kafka hashes the key to deterministically
pick a partition, guaranteeing that all messages with the same key are
ordered relative to each other.

Concepts introduced:
  - Partitions
  - Message keys
  - Partition assignment

Prerequisites:
  docker compose up -d
  # Create a topic with 3 partitions (auto-create uses 1 by default):
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --create --topic partitioned-topic \
      --partitions 3 --replication-factor 1

Run:
    python -m examples.02_partitions
"""

import json
import logging
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "partitioned-topic"
USERS = ["alice", "bob", "carol", "dave", "eve"]


def produce_keyed_messages() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    events = [
        ("alice", "login"),
        ("bob", "login"),
        ("alice", "view_product"),
        ("carol", "login"),
        ("bob", "add_to_cart"),
        ("alice", "checkout"),
        ("carol", "view_product"),
        ("dave", "login"),
        ("bob", "logout"),
        ("alice", "logout"),
    ]

    logger.info("Producing %d keyed messages …", len(events))
    for user, event in events:
        producer.send(TOPIC, key=user, value={"user": user, "event": event})

    producer.flush()
    producer.close()
    logger.info("All messages produced.")


def consume_and_show_partitions() -> dict[int, list]:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="partitions-demo-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",
        consumer_timeout_ms=3_000,
    )

    by_partition: dict[int, list] = defaultdict(list)

    for record in consumer:
        by_partition[record.partition].append(
            {"key": record.key, "event": record.value.get("event")}
        )

    consumer.close()
    return dict(by_partition)


def main() -> None:
    logger.info("=== Example 02: Partitions & Message Keys ===")
    produce_keyed_messages()
    partition_map = consume_and_show_partitions()

    if not partition_map:
        logger.warning("No messages received – is Kafka running with the topic created?")
        return

    logger.info("Messages grouped by partition:")
    for partition, records in sorted(partition_map.items()):
        logger.info("  Partition %d (%d messages):", partition, len(records))
        for r in records:
            logger.info("    key=%-8s  event=%s", r["key"], r["event"])

    # Verify: all messages for the same user land in the same partition.
    user_partitions: dict[str, set] = defaultdict(set)
    for partition, records in partition_map.items():
        for r in records:
            if r["key"]:
                user_partitions[r["key"]].add(partition)

    logger.info("Partition consistency check (each user should appear in exactly 1 partition):")
    all_consistent = True
    for user, partitions in sorted(user_partitions.items()):
        consistent = len(partitions) == 1
        if not consistent:
            all_consistent = False
        logger.info("  %-8s -> partitions %s  %s", user, partitions, "✓" if consistent else "✗")

    if all_consistent:
        logger.info("All users consistently hashed to a single partition.")


if __name__ == "__main__":
    main()
