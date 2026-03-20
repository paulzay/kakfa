# Kafka Learning Project

A hands-on Python project for learning [Apache Kafka](https://kafka.apache.org/) from first principles.

---

## What is Kafka?

Apache Kafka is a **distributed event-streaming platform** built for high-throughput, fault-tolerant, real-time data pipelines.

| Concept | Description |
|---|---|
| **Topic** | A named stream of records – like a table in a database or a folder in a filesystem. |
| **Partition** | A topic is split into ordered, immutable partitions for parallelism. |
| **Offset** | The position of a record within a partition. Consumers track their own offsets. |
| **Producer** | Writes records to topics. |
| **Consumer** | Reads records from topics. |
| **Consumer Group** | A set of consumers that share the work of reading a topic. Kafka assigns each partition to exactly one member of the group. |
| **Broker** | A single Kafka server. A cluster has multiple brokers. |

---

## Project Structure

```
kakfa/
├── docker-compose.yml         # Local Kafka broker (KRaft, no Zookeeper) + Kafka-UI
├── requirements.txt           # Python dependencies
├── producer/
│   ├── simple_producer.py     # Basic JSON producer
│   └── advanced_producer.py   # Keys, headers, batching, async callbacks
├── consumer/
│   ├── simple_consumer.py     # Basic auto-commit consumer
│   └── consumer_group.py      # Manual-commit, multi-instance group demo
├── examples/
│   ├── 01_hello_kafka.py      # Produce → consume round-trip
│   ├── 02_partitions.py       # Message keys & partition routing
│   └── 03_consumer_groups.py  # Load-balanced consumer group
└── tests/
    └── test_kafka.py          # Unit tests (no live broker needed)
```

---

## Prerequisites

| Tool | Version |
|---|---|
| Python | 3.10 + |
| Docker & Docker Compose | any recent version |

---

## Quick Start

### 1 – Clone and install Python dependencies

```bash
git clone https://github.com/paulzay/kakfa.git
cd kakfa
pip install -r requirements.txt
```

### 2 – Start the local Kafka broker

```bash
docker compose up -d
```

This starts:
- **kafka** on `localhost:9092` (KRaft mode – no Zookeeper required)
- **kafka-ui** on <http://localhost:8080> – a web UI to browse topics, partitions, and messages

Wait ~15 seconds for Kafka to become healthy:

```bash
docker compose ps   # STATUS should show "healthy"
```

### 3 – Run the examples (in order)

```bash
# Example 01 – simplest possible round-trip
python -m examples.01_hello_kafka

# Example 02 – partitions & message keys
# First create a topic with 3 partitions:
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic partitioned-topic \
    --partitions 3 --replication-factor 1

python -m examples.02_partitions

# Example 03 – consumer groups
# Create a topic with 4 partitions:
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic group-demo-topic \
    --partitions 4 --replication-factor 1

python -m examples.03_consumer_groups
```

---

## Modules in Detail

### `producer/simple_producer.py`

Produces five JSON events to `simple-topic`.  
Key settings explained:
- `value_serializer` – converts a Python dict to JSON bytes automatically
- `acks="all"` – wait for all in-sync replicas to confirm the write (safest)
- `retries=3` – retry up to three times on transient failures

```bash
python -m producer.simple_producer
```

### `producer/advanced_producer.py`

Shows keyed messages, custom headers, batching, and async callbacks.  
Key settings:
- `key_serializer` – encode the key as UTF-8 bytes
- `linger_ms=10` – accumulate messages for 10 ms before flushing a batch
- `compression_type="gzip"` – compress batches to reduce network traffic

```bash
python -m producer.advanced_producer
```

### `consumer/simple_consumer.py`

Subscribes to `simple-topic` and logs every record.  
Key settings:
- `auto_offset_reset="earliest"` – start from the beginning if no committed offset
- `enable_auto_commit=True` – Kafka commits offsets every 5 s automatically

```bash
python -m consumer.simple_consumer
```

Press **Ctrl-C** to stop.

### `consumer/consumer_group.py`

Run this in two separate terminals to watch Kafka split partitions between them:

```bash
# Terminal 1
python -m consumer.consumer_group --instance A

# Terminal 2
python -m consumer.consumer_group --instance B
```

Then produce messages and observe which consumer handles which partition.
Uses **manual offset commit** (`enable_auto_commit=False`) so an offset is
only saved *after* the message is successfully processed.

---

## Running the Tests

No live Kafka broker is required – the tests mock all I/O:

```bash
python -m pytest tests/ -v
```

---

## Useful Kafka CLI Commands

All commands run inside the `kafka` container:

```bash
# List topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list

# Describe a topic (shows partitions, replicas, leaders)
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe --topic simple-topic

# Consume from the beginning (built-in console consumer)
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic simple-topic --from-beginning

# List consumer groups
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --list

# Check consumer-group offsets and lag
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe --group simple-consumer-group
```

---

## Stopping Kafka

```bash
docker compose down
```

---

## Key Takeaways

1. **Topics are append-only logs** – records are never overwritten; consumers read at their own pace.
2. **Partitions enable parallelism** – more partitions → more consumer instances can work in parallel.
3. **Message keys guarantee ordering** – all records with the same key land in the same partition, so they are processed in order.
4. **Consumer groups share work** – Kafka assigns each partition to exactly one consumer in the group; if a member fails, Kafka rebalances automatically.
5. **Offsets give you control** – auto-commit is convenient; manual commit lets you ensure "at-least-once" or "exactly-once" processing semantics.
