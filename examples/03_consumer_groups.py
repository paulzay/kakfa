"""
Example 03 – Consumer Groups

Multiple consumers sharing the same *group_id* form a *consumer group*.
Kafka automatically assigns partitions across group members, so the work
is spread evenly.  If one member dies, Kafka *rebalances* and the
remaining members pick up its partitions.

Concepts introduced:
  - Consumer groups
  - Partition rebalancing
  - group_id
  - Manual vs automatic offset commits

This script simulates two consumers in the same group by using two
KafkaConsumer instances in the same process (not common in production but
fine for a demo).

Prerequisites:
  docker compose up -d
  # Create a topic with 4 partitions:
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --create --topic group-demo-topic \
      --partitions 4 --replication-factor 1

Run:
    python -m examples.03_consumer_groups
"""

import json
import logging
import threading
import time
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)-20s  %(message)s",
)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "group-demo-topic"
GROUP_ID = "example-03-group"
NUM_MESSAGES = 20


def produce_messages(n: int = NUM_MESSAGES) -> None:
    logger = logging.getLogger("producer")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    users = ["alice", "bob", "carol", "dave"]
    for i in range(n):
        user = users[i % len(users)]
        producer.send(TOPIC, key=user, value={"id": i, "user": user})

    producer.flush()
    producer.close()
    logger.info("Produced %d messages.", n)


def run_consumer_instance(instance_id: str, results: dict, stop_event: threading.Event) -> None:
    """Consume messages until *stop_event* is set or timeout."""
    logger = logging.getLogger(f"consumer-{instance_id}")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        client_id=f"{GROUP_ID}-{instance_id}",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=5_000,
    )

    received = []
    assigned_partitions = set()

    logger.info("Started – waiting for partition assignment …")
    try:
        for record in consumer:
            if stop_event.is_set():
                break
            assigned_partitions.update(
                tp.partition for tp in consumer.assignment()
            )
            logger.info(
                "partition=%d  offset=%d  key=%-6s  id=%d",
                record.partition,
                record.offset,
                record.key,
                record.value["id"],
            )
            received.append(record.value)
    finally:
        consumer.close()

    results[instance_id] = {
        "received": received,
        "partitions": sorted(assigned_partitions),
    }
    logger.info(
        "Closed. Received %d messages from partitions %s.",
        len(received),
        sorted(assigned_partitions),
    )


def main() -> None:
    logger = logging.getLogger("main")
    logger.info("=== Example 03: Consumer Groups ===")

    logger.info("--- Producing %d messages ---", NUM_MESSAGES)
    produce_messages()

    # Give the broker a moment to settle.
    time.sleep(1)

    logger.info("--- Starting two consumers in group '%s' ---", GROUP_ID)
    results: dict = {}
    stop_event = threading.Event()

    t1 = threading.Thread(
        target=run_consumer_instance, args=("A", results, stop_event), daemon=True
    )
    t2 = threading.Thread(
        target=run_consumer_instance, args=("B", results, stop_event), daemon=True
    )

    t1.start()
    # Small delay so both consumers are alive during Kafka's rebalance.
    time.sleep(0.5)
    t2.start()

    t1.join()
    t2.join()

    if not results:
        logger.warning("No results – is Kafka running with the topic created?")
        return

    logger.info("--- Summary ---")
    total = 0
    for instance_id, data in sorted(results.items()):
        count = len(data["received"])
        total += count
        logger.info(
            "Consumer %s: %d messages  partitions=%s",
            instance_id,
            count,
            data["partitions"],
        )
    logger.info("Total messages received across all consumers: %d / %d", total, NUM_MESSAGES)


if __name__ == "__main__":
    main()
