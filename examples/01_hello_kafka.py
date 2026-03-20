"""
Example 01 – Hello Kafka!

This is the simplest possible Kafka program:
  1. Produce one message to a topic.
  2. Consume it back and print it.

Concepts introduced:
  - KafkaProducer / KafkaConsumer
  - Topics
  - Serialisation / deserialisation

Prerequisites:
  docker compose up -d

Run:
    python -m examples.01_hello_kafka
"""

import json
import logging
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "hello-kafka"


def produce_hello() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    message = {"greeting": "Hello, Kafka!", "from": "example-01"}
    producer.send(TOPIC, value=message)
    producer.flush()
    producer.close()
    logger.info("Produced: %s", message)


def consume_hello() -> dict:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="hello-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        # Stop polling after 3 s of silence (good for one-shot scripts).
        consumer_timeout_ms=3_000,
    )

    received = None
    for record in consumer:
        logger.info(
            "Consumed from partition=%d offset=%d: %s",
            record.partition,
            record.offset,
            record.value,
        )
        received = record.value
        break  # only need the first message for this demo

    consumer.close()
    return received


def main() -> None:
    logger.info("=== Example 01: Hello Kafka! ===")
    produce_hello()
    result = consume_hello()
    if result:
        logger.info("Round-trip complete. Message: %s", result)
    else:
        logger.warning("No message received – is Kafka running?")


if __name__ == "__main__":
    main()
