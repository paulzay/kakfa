"""
Unit tests for the producer and consumer modules.

These tests use unittest.mock to replace the real KafkaProducer /
KafkaConsumer so that no live Kafka broker is required.
"""

import json
import unittest
from unittest.mock import MagicMock, patch, call

from producer.simple_producer import create_producer, send_message
from producer.advanced_producer import (
    create_advanced_producer,
    on_send_success,
    on_send_error,
    send_with_key,
)
from consumer.simple_consumer import create_consumer, consume_messages


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(topic="test-topic", partition=0, offset=0, key=None, value=None):
    """Return a minimal mock that looks like a ConsumerRecord."""
    record = MagicMock()
    record.topic = topic
    record.partition = partition
    record.offset = offset
    record.key = key
    record.value = value or {}
    return record


def _make_metadata(topic="test-topic", partition=0, offset=0):
    meta = MagicMock()
    meta.topic = topic
    meta.partition = partition
    meta.offset = offset
    return meta


# ---------------------------------------------------------------------------
# Producer tests
# ---------------------------------------------------------------------------

class TestSimpleProducer(unittest.TestCase):

    @patch("producer.simple_producer.KafkaProducer")
    def test_create_producer_uses_json_serialiser(self, mock_cls):
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance

        producer = create_producer("broker:9092")

        mock_cls.assert_called_once()
        kwargs = mock_cls.call_args.kwargs
        self.assertEqual(kwargs["bootstrap_servers"], "broker:9092")
        self.assertEqual(kwargs["acks"], "all")
        self.assertEqual(kwargs["retries"], 3)
        # Serialiser should encode a dict to JSON bytes.
        serialiser = kwargs["value_serializer"]
        self.assertEqual(serialiser({"k": "v"}), json.dumps({"k": "v"}).encode())

    @patch("producer.simple_producer.KafkaProducer")
    def test_send_message_calls_send_and_get(self, mock_cls):
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = _make_metadata(partition=1, offset=42)
        mock_producer.send.return_value = mock_future
        mock_cls.return_value = mock_producer

        producer = create_producer()
        send_message(producer, "my-topic", {"hello": "world"}, key="k1")

        mock_producer.send.assert_called_once_with(
            "my-topic", value={"hello": "world"}, key=b"k1"
        )
        mock_future.get.assert_called_once_with(timeout=10)

    @patch("producer.simple_producer.KafkaProducer")
    def test_send_message_no_key(self, mock_cls):
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = _make_metadata()
        mock_producer.send.return_value = mock_future

        send_message(mock_producer, "topic", {"x": 1}, key=None)

        _, kwargs = mock_producer.send.call_args
        self.assertIsNone(kwargs["key"])

    @patch("producer.simple_producer.KafkaProducer")
    def test_send_message_raises_on_kafka_error(self, mock_cls):
        from kafka.errors import KafkaError

        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.side_effect = KafkaError("broker unavailable")
        mock_producer.send.return_value = mock_future

        with self.assertRaises(KafkaError):
            send_message(mock_producer, "topic", {"x": 1})


class TestAdvancedProducer(unittest.TestCase):

    @patch("producer.advanced_producer.KafkaProducer")
    def test_create_advanced_producer_config(self, mock_cls):
        create_advanced_producer("broker:9092")
        kwargs = mock_cls.call_args.kwargs
        self.assertEqual(kwargs["compression_type"], "gzip")
        self.assertEqual(kwargs["linger_ms"], 10)
        self.assertEqual(kwargs["batch_size"], 16_384)

    def test_on_send_success_logs(self):
        meta = _make_metadata(topic="t", partition=2, offset=99)
        # Should not raise.
        on_send_success(meta)

    def test_on_send_error_logs(self):
        on_send_error(Exception("boom"))

    def test_send_with_key_attaches_callbacks(self):
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.add_callback.return_value = mock_future
        mock_future.add_errback.return_value = mock_future
        mock_producer.send.return_value = mock_future

        send_with_key(mock_producer, "topic", "alice", {"event": "login"})

        mock_producer.send.assert_called_once_with(
            "topic", key="alice", value={"event": "login"}
        )
        mock_future.add_callback.assert_called_once_with(on_send_success)
        mock_future.add_errback.assert_called_once_with(on_send_error)


# ---------------------------------------------------------------------------
# Consumer tests
# ---------------------------------------------------------------------------

class TestSimpleConsumer(unittest.TestCase):

    @patch("consumer.simple_consumer.KafkaConsumer")
    def test_create_consumer_config(self, mock_cls):
        create_consumer(
            topic="my-topic",
            group_id="my-group",
            bootstrap_servers="broker:9092",
            auto_offset_reset="latest",
        )
        mock_cls.assert_called_once()
        args, kwargs = mock_cls.call_args
        self.assertIn("my-topic", args)
        self.assertEqual(kwargs["group_id"], "my-group")
        self.assertEqual(kwargs["bootstrap_servers"], "broker:9092")
        self.assertEqual(kwargs["auto_offset_reset"], "latest")
        self.assertTrue(kwargs["enable_auto_commit"])

    @patch("consumer.simple_consumer.KafkaConsumer")
    def test_consume_messages_max_messages(self, mock_cls):
        records = [
            _make_record(partition=0, offset=i, value={"id": i})
            for i in range(5)
        ]
        mock_consumer = MagicMock()
        mock_consumer.__iter__ = MagicMock(return_value=iter(records))
        mock_cls.return_value = mock_consumer

        consumer = create_consumer()
        received = consume_messages(consumer, max_messages=3)

        self.assertEqual(len(received), 3)
        self.assertEqual(received[0], {"id": 0})
        self.assertEqual(received[2], {"id": 2})
        mock_consumer.close.assert_called_once()

    @patch("consumer.simple_consumer.KafkaConsumer")
    def test_consume_messages_returns_all_when_no_limit(self, mock_cls):
        records = [_make_record(value={"id": i}) for i in range(4)]
        mock_consumer = MagicMock()
        mock_consumer.__iter__ = MagicMock(return_value=iter(records))
        mock_cls.return_value = mock_consumer

        consumer = create_consumer()
        received = consume_messages(consumer)

        self.assertEqual(len(received), 4)

    @patch("consumer.simple_consumer.KafkaConsumer")
    def test_consume_messages_closes_on_error(self, mock_cls):
        from kafka.errors import KafkaError

        mock_consumer = MagicMock()
        mock_consumer.__iter__ = MagicMock(side_effect=KafkaError("err"))
        mock_cls.return_value = mock_consumer

        consumer = create_consumer()
        received = consume_messages(consumer)

        self.assertEqual(received, [])
        mock_consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
