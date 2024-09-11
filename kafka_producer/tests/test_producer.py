import pytest
from unittest.mock import MagicMock, patch
from kafka.errors import KafkaError
from src.producer import CustomKafkaProducer  # Update this import path as needed


@pytest.fixture
def mock_kafka_producer():
    with patch("src.producer.KafkaProducer") as mock:  # Update this path
        yield mock


@pytest.fixture
def mock_config():
    with patch("src.producer.config") as mock:  # Update this path
        mock.MAX_RETRIES = 3
        mock.RETRY_DELAY = 1
        mock.TOPIC_NAME = "test_topic"
        yield mock


@pytest.fixture
def mock_logger():
    with patch("src.producer.setup_logging") as mock_setup:  # Update this path
        mock_logger = MagicMock()
        mock_setup.return_value = mock_logger
        yield mock_logger


@pytest.fixture
def producer(mock_kafka_producer, mock_config, mock_logger):
    return CustomKafkaProducer("localhost:9092")


def test_successful_connection(producer, mock_kafka_producer, mock_logger):
    assert producer.producer is not None
    mock_kafka_producer.assert_called_once()
    mock_logger.info.assert_called_with("Successfully connected to Kafka broker.")


def test_connection_failure(mock_kafka_producer, mock_config, mock_logger):
    mock_kafka_producer.side_effect = KafkaError("Connection failed")
    producer = CustomKafkaProducer("localhost:9092")
    assert producer.producer is None
    mock_logger.error.assert_called_with(
        "Failed to connect to Kafka broker after maximum retries."
    )


def test_publish_message_success(producer, mock_config):
    mock_send = MagicMock()
    producer.producer.send = mock_send

    message = {"key": "value"}
    producer.publish_message(message)

    mock_send.assert_called_once_with(mock_config.TOPIC_NAME, value=message)


def test_publish_message_failure(producer, mock_logger):
    producer.producer.send.side_effect = KafkaError("Send failed")

    with pytest.raises(KafkaError):
        producer.publish_message({"key": "value"})
    mock_logger.error.assert_called_with(
        "Failed to produce message: KafkaError: Send failed"
    )


def test_close_producer(producer, mock_logger):
    producer.close()
    producer.producer.flush.assert_called_once()
    producer.producer.close.assert_called_once()
    mock_logger.info.assert_called_with("Kafka producer closed.")


def test_on_send_success(producer, mock_logger):
    record_metadata = MagicMock()
    record_metadata.topic = "test_topic"
    record_metadata.partition = 0
    record_metadata.offset = 1
    producer.on_send_success(record_metadata)
    mock_logger.info.assert_called_with(
        "Message delivered to test_topic partition 0 offset 1"
    )


def test_on_send_error(producer, mock_logger):
    exception = Exception("Test exception")
    producer.on_send_error(exception)
    mock_logger.error.assert_called_with("Error in message delivery: Test exception")


if __name__ == "__main__":
    pytest.main()
