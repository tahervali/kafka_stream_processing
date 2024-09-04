from kafka import KafkaProducer
from kafka.errors import KafkaError
import os
import time
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "broker:9092")
TOPIC_NAME = "test_topic"
MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds


class CustomKafkaProducer:
    def __init__(self, bootstrap_servers):
        self.producer = None
        self.bootstrap_servers = bootstrap_servers
        self.connect()

    def connect(self):
        retries = 0
        while retries < MAX_RETRIES:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
                logger.info("Connected to Kafka broker")
                break
            except KafkaError as e:
                logger.error(f"Failed to connect to Kafka broker: {e}")
                retries += 1
                time.sleep(RETRY_DELAY)

        if self.producer is None:
            raise Exception("Failed to connect to Kafka broker after multiple attempts")

    def produce_message(self, message):
        try:
            self.producer.send(TOPIC_NAME, value=message)
            self.producer.flush()
            logger.info(f"Produced message: {message}")
        except KafkaError as e:
            logger.error(f"Failed to produce message: {e}")


if __name__ == "__main__":
    producer = CustomKafkaProducer(KAFKA_SERVER)

    while True:
        message = {"timestamp": time.time(), "message": "Hello, Kafka!"}
        producer.produce_message(message)
        time.sleep(float(os.getenv("PRODUCER_INTERVAL", 1)))
