from unittest.mock import MagicMock, patch

import pytest
from kafka.errors import KafkaError, NoBrokersAvailable

from drunc.broadcast.server.kafka_sender import KafkaSender
from drunc.exceptions import DruncSetupException


@pytest.fixture
def mock_kafka_producer():
    """Fixture to mock a KafkaProducer class and its instance."""
    with patch("kafka.KafkaProducer") as mock_class:
        mock_instance = mock_class.return_value
        yield mock_class, mock_instance


@pytest.fixture
def mock_sender(mock_kafka_producer):
    """Fixture to create a KafkaSender instance with a mocked KafkaProducer."""
    kafka_sender = KafkaSender(
        kafka_address="test-kafka-address", publish_timeout=5, topic="test-topic"
    )
    kafka_sender._log = MagicMock()
    return kafka_sender


def test_init_raises_drunc_setup_exception():
    """Test that KafkaSender raises DruncSetupException when KafkaProducer cannot connect to a broker."""
    mock_logger = MagicMock()
    kafka_address = "test-address"
    expected_exc_msg = f"{kafka_address} does not seem to point to a kafka broker."

    with (
        patch("logging.getLogger", return_value=mock_logger),
        patch("kafka.KafkaProducer", side_effect=NoBrokersAvailable),
    ):
        with pytest.raises(DruncSetupException) as exc_info:
            KafkaSender(
                kafka_address=kafka_address, publish_timeout=5, topic="test-topic"
            )

    mock_logger.critical.assert_called_once()
    log_call_args = mock_logger.critical.call_args[0][0]
    assert expected_exc_msg in log_call_args
    assert expected_exc_msg in str(exc_info.value)


def test_send_success(mock_sender, mock_kafka_producer):
    """Test that KafkaSender._send successfully sends a message and logs the metadata."""
    _, mock_instance = mock_kafka_producer
    sender = mock_sender
    sender._log = MagicMock()

    mock_future = MagicMock()
    mock_future.get.return_value = "test-metadata"
    mock_instance.send.return_value = mock_future

    mock_broadcast_msg = MagicMock()
    mock_broadcast_msg.SerializeToString.return_value = b"test-msg"
    sender._send(mock_broadcast_msg)

    mock_instance.send.assert_called_with("test-topic", b"test-msg")
    sender._log.debug.assert_called_with("test-metadata published")


def test_send_handle_exception(mock_sender, mock_kafka_producer):
    """Test that KafkaSender._send handles KafkaError exceptions and logs the error."""
    _, mock_instance = mock_kafka_producer
    sender = mock_sender

    mock_future = MagicMock()
    mock_future.get.side_effect = KafkaError("Connection lost")
    mock_instance.send.return_value = mock_future

    mock_broadcast_msg = MagicMock()
    mock_broadcast_msg.SerializeToString.return_value = b"test-broadcast-msg"

    sender._log = MagicMock()

    sender._send(mock_broadcast_msg)

    sender._log.error.assert_called()
    log_message = sender._log.error.call_args[0][0]
    assert "Connection lost" in log_message


def test_describe_broadcast(mock_sender):
    """Test that KafkaSender.describe_broadcast returns the correct information."""

    result = mock_sender.describe_broadcast()

    assert result.topic == "test-topic"
    assert result.kafka_address == "test-kafka-address"
