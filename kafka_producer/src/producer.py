import json
import threading
import time
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.config import config
from src.utils import setup_logging, generate_log_data


class CustomKafkaProducer:
    def __init__(self, bootstrap_servers: str):
        """
        Initializes the CustomKafkaProducer with the given Kafka bootstrap servers.

        Parameters:
            bootstrap_servers (str): Address of the Kafka bootstrap server(s).
        """
        self.bootstrap_servers = bootstrap_servers
        self.logger = setup_logging(config.LOG_LEVEL)
        self.producer: Optional[KafkaProducer] = self.connect()

    def connect(self) -> Optional[KafkaProducer]:
        """
        Attempts to connect to the Kafka broker with retry logic.
        If the connection fails after MAX_RETRIES, an exception is raised.

        Returns:
            KafkaProducer: A connected KafkaProducer instance, or None if unable to connect.
        """
        retries = 0
        while retries < config.MAX_RETRIES:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks="all",  # Ensure message delivery guarantee
                    linger_ms=5,  # Wait a small-time to batch records
                    batch_size=16384,  # (default is 16KB, can be increased)
                )
                self.logger.info("Successfully connected to Kafka broker.")
                return producer
            except KafkaError as kafkaError:
                retries += 1
                self.logger.warning(
                    f"Failed to connect to Kafka broker (attempt "
                    f"{retries}/{config.MAX_RETRIES}): {kafkaError}"
                )
                time.sleep(config.RETRY_DELAY)

        self.logger.error("Failed to connect to Kafka broker after maximum retries.")
        return None

    def publish_message(self, message: dict):
        """
        Publishes a message asynchronously to the Kafka topic.

        Parameters:
            message (dict): The message to be sent to the Kafka topic.
        """
        if not self.producer:
            self.logger.error("Kafka producer is not connected.")
            return

        try:
            self.producer.send(config.TOPIC_NAME, value=message).add_callback(
                self.on_send_success
            ).add_errback(self.on_send_error)
        except KafkaError as kafka_error:
            self.logger.error(f"Failed to produce message: {kafka_error}")
            raise

    def on_send_success(self, record_metadata):
        self.logger.info(
            f"Message delivered to {record_metadata.topic} partition {record_metadata.partition} "
            f"offset {record_metadata.offset}"
        )

    def on_send_error(self, exception):
        self.logger.error(f"Error in message delivery: {exception}")

    def close(self):
        """
        Closes the Kafka producer connection gracefully.
        """
        if self.producer:
            self.producer.flush()  # Ensure all messages are sent
            self.producer.close()
            self.logger.info("Kafka producer closed.")


def produce_batch(kafka_producer: CustomKafkaProducer, num_messages: int):
    """
    Produces a batch of messages to Kafka.

    Parameters:
        kafka_producer (CustomKafkaProducer): Kafka producer instance.
        num_messages (int): Number of messages to produce in a batch.
    """
    for _ in range(num_messages):
        log_data = generate_log_data()  # Generate a new log data record
        kafka_producer.publish_message(log_data)


def start_producing(kafka_producer: CustomKafkaProducer):
    """
    Start continuous message production using threads.

    Parameters:
        kafka_producer (CustomKafkaProducer): Kafka producer instance.
    """
    while True:
        threads = []
        for _ in range(config.THREAD_COUNT):
            thread = threading.Thread(
                target=produce_batch, args=(kafka_producer, config.BATCH_SIZE)
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()  # Wait for all threads to finish before starting a new batch

        kafka_producer.logger.debug(
            f"Batch of {config.THREAD_COUNT * config.BATCH_SIZE} messages produced."
        )
        time.sleep(
            config.PRODUCER_INTERVAL
        )  # Short interval before producing the next batch


if __name__ == "__main__":
    producer_instance = None
    try:
        # Create Kafka producer
        producer_instance = CustomKafkaProducer(config.KAFKA_SERVER)

        if (
            producer_instance.producer
        ):  # Ensure producer is connected before starting production
            start_producing(producer_instance)
        else:
            producer_instance.logger.error("Failed to start Kafka producer. Exiting...")

    except Exception as e:
        if producer_instance:
            producer_instance.logger.error(f"An error occurred: {e}")
    finally:
        # Ensure the producer connection is closed on exit
        if producer_instance:
            producer_instance.close()
