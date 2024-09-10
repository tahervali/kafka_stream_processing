import json

import pytest
from unittest.mock import patch, MagicMock
from kafka.errors import KafkaError
from producer import CustomKafkaProducer, generate_log_data
from config import config


@pytest.fixture
def mock_producer():
    with patch('producer.KafkaProducer') as MockKafkaProducer:
        yield MockKafkaProducer


@pytest.fixture
def producer_instance(mock_producer):
    mock_producer.return_value = MagicMock()
    return CustomKafkaProducer(config.KAFKA_SERVER)


def test_connect_success(producer_instance, mock_producer):
    # Test successful connection
    assert producer_instance.producer is not None
    mock_producer.assert_called_once_with(
        bootstrap_servers=config.KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def test_connect_failure(producer_instance, mock_producer):
    # Test connection retries and failure
    mock_producer.side_effect = KafkaError("Connection error")
    with pytest.raises(KafkaError):
        producer_instance.connect()
    assert mock_producer.call_count == config.MAX_RETRIES


def test_publish_message_success(producer_instance):
    # Test successful message publishing
    mock_producer_instance = producer_instance.producer
    message = generate_log_data()
    producer_instance.publish_message(message)
    mock_producer_instance.send.assert_called_once_with(config.TOPIC_NAME, value=message)
    mock_producer_instance.flush.assert_called_once()


def test_publish_message_failure(producer_instance):
    # Test message publishing failure
    mock_producer_instance = producer_instance.producer
    mock_producer_instance.send.side_effect = KafkaError("Send error")
    message = generate_log_data()
    with pytest.raises(KafkaError):
        producer_instance.publish_message(message)


def test_close(producer_instance):
    # Test closing the producer
    mock_producer_instance = producer_instance.producer
    producer_instance.close()
    mock_producer_instance.close.assert_called_once()
