import pytest
from unittest.mock import MagicMock, patch
from kafka.errors import KafkaError
from src.producer import CustomKafkaProducer, produce_batch, start_producing


# Mock the configuration values
mock_config = {
    "KAFKA_SERVER": "broker:9092",
    "TOPIC_NAME": "view_log",
    "MAX_RETRIES": 3,
    "RETRY_DELAY": 1,
    "PRODUCER_INTERVAL": 1,
    "THREAD_COUNT": 2,
    "BATCH_SIZE": 2,
    "LOG_LEVEL": "INFO",
}


@pytest.fixture
def mock_logger():
    with patch("src.producer.setup_logging") as mock_setup_logging:
        logger = MagicMock()
        mock_setup_logging.return_value = logger
        yield logger


@pytest.fixture
def mock_kafka_producer(mock_logger):
    with patch("src.producer.CustomKafkaProducer") as MockKafkaProducer:
        producer_instance = MockKafkaProducer.return_value
        producer_instance.send.return_value.add_callback.return_value = None
        producer_instance.send.return_value.add_errback.return_value = None
        return producer_instance


@patch("src.producer.config", new=MagicMock(**mock_config))
def test_connect_success(mock_kafka_producer, mock_logger):
    # Test successful Kafka connection
    producer = CustomKafkaProducer(mock_config["KAFKA_SERVER"])
    assert producer.producer is not None
    mock_logger.info.assert_called_with("Successfully connected to Kafka broker.")



@patch("src.producer.config", new=MagicMock(**mock_config))
def test_connect_failure(mock_logger):
    # Patch the KafkaProducer instantiation inside the connect method to raise KafkaError
    with patch("kafka.KafkaProducer", side_effect=KafkaError("Connection failed")) as mock_kafka_producer:

        # Initialize the CustomKafkaProducer
        producer = CustomKafkaProducer(mock_config["KAFKA_SERVER"])

        # Ensure producer is None after failed retries
        assert producer.producer is None

        # Check if the logger's warning method was called the expected number of times (equal to MAX_RETRIES)
        assert mock_logger.warning.call_count == mock_config["MAX_RETRIES"]

        # Verify that an error log was recorded after the retries were exhausted
        mock_logger.error.assert_called_with("Failed to connect to Kafka broker after maximum retries.")

        # Ensure KafkaProducer was called for each retry attempt
        assert mock_kafka_producer.call_count == mock_config["MAX_RETRIES"]

#
# @patch("src.producer.config", new=MagicMock(**mock_config))
# def test_publish_message_success(mock_kafka_producer, mock_logger):
#     # Test message publishing success
#     producer = CustomKafkaProducer(mock_config["KAFKA_SERVER"])
#     message = {"view_id": "test_view_id"}
#
#     producer.publish_message(message)
#
#     mock_kafka_producer.send.assert_called_with(mock_config["TOPIC_NAME"], value=message)
#     mock_logger.error.assert_not_called()
#
#
# @patch("src.producer.config", new=MagicMock(**mock_config))
# def test_publish_message_failure(mock_kafka_producer, mock_logger):
#     # Simulate KafkaError when publishing message
#     producer = CustomKafkaProducer(mock_config["KAFKA_SERVER"])
#     mock_kafka_producer.send.side_effect = KafkaError("Failed to send message")
#
#     with pytest.raises(KafkaError):
#         producer.publish_message({"view_id": "test_view_id"})
#
#     mock_logger.error.assert_called_with("Failed to produce message: Failed to send message")
#
#
# @patch("src.producer.config", new=MagicMock(**mock_config))
# def test_close(mock_kafka_producer, mock_logger):
#     # Test closing the Kafka producer
#     producer = CustomKafkaProducer(mock_config["KAFKA_SERVER"])
#     producer.close()
#
#     mock_kafka_producer.flush.assert_called_once()
#     mock_kafka_producer.close.assert_called_once()
#     mock_logger.info.assert_called_with("Kafka producer closed.")
#
#
# @patch("src.producer.config", new=MagicMock(**mock_config))
# def test_produce_batch(mock_kafka_producer, mock_logger):
#     # Test producing a batch of messages
#     producer = CustomKafkaProducer(mock_config["KAFKA_SERVER"])
#     with patch("src.producer.generate_log_data", return_value={"view_id": "test_view_id"}):
#         produce_batch(producer, 3)
#         assert mock_kafka_producer.send.call_count == 3
#
#
# @patch("src.producer.config", new=MagicMock(**mock_config))
# def test_start_producing(mock_kafka_producer, mock_logger):
#     # Test the continuous message production using threads
#     producer = CustomKafkaProducer(mock_config["KAFKA_SERVER"])
#
#     with patch("threading.Thread.start"):
#         start_producing(producer)
#         mock_logger.debug.assert_called_with(
#             f"Batch of {mock_config['THREAD_COUNT'] * mock_config['BATCH_SIZE']} messages produced."
#         )
