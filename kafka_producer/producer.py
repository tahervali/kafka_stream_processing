import json
import random
import time
import uuid
from datetime import timedelta, datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import KAFKA_SERVER, TOPIC_NAME, MAX_RETRIES, RETRY_DELAY, PRODUCER_INTERVAL, LOG_LEVEL
from utils import setup_logging


def generate_log_data():
    """
    Generates a simulated log record according to the specified schema.

    Returns:
        dict: A dictionary containing the log data with the following fields:
            - view_id (str): Unique ID of the record.
            - start_timestamp (str): Timestamp when the view started.
            - end_timestamp (str): Timestamp when the view ended (and pushed to Kafka).
            - banner_id (int): Randomly generated banner ID.
            - campaign_id (int): Randomly generated campaign ID (from 10 to 140 in increments of 10).
    """
    view_id = str(uuid.uuid4())  # Generate a unique ID for the record
    start_timestamp = datetime.now().replace(microsecond=0)
    end_timestamp = (start_timestamp + timedelta(seconds=random.randint(1, 300))).replace(microsecond=0)
    banner_id = random.randint(1, 100000)  # Generate a random banner ID
    campaign_id = random.choice(range(10, 141, 10))  # Generate a random campaign ID

    return {
        "view_id": view_id,
        "start_timestamp": start_timestamp.isoformat(),
        "end_timestamp": end_timestamp.isoformat(),
        "banner_id": banner_id,
        "campaign_id": campaign_id,
    }


class CustomKafkaProducer:
    def __init__(self, bootstrap_servers):
        """
        Initializes the CustomKafkaProducer with the given Kafka bootstrap servers.

        Args:
            bootstrap_servers (str): Address of the Kafka bootstrap server(s).
        """
        self.bootstrap_servers = bootstrap_servers
        self.producer = self.connect()

    def connect(self):
        """
        Attempts to connect to the Kafka broker with retry logic.
        If the connection fails after MAX_RETRIES, an exception is raised.

        Returns:
            KafkaProducer: A connected KafkaProducer instance.
        """
        retries = 0
        producer = None

        while retries < MAX_RETRIES:
            try:
                # Initialize the Kafka producer with JSON serialization
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
                logger.info("Successfully connected to Kafka broker.")
                return producer
            except KafkaError as e:
                retries += 1
                logger.error(f"Failed to connect to Kafka broker (attempt {retries}/{MAX_RETRIES}): {e}")
                time.sleep(RETRY_DELAY)

        raise KafkaError("Failed to connect to Kafka broker after maximum retries.")

    def publish_message(self, message):
        """
        Publishes a message to the Kafka topic.

        Args:
            message (dict): The message to be sent to the Kafka topic.
        """
        try:
            # Send the message to the Kafka topic
            self.producer.send(TOPIC_NAME, value=message)
            self.producer.flush()  # Ensure all messages are sent immediately
            logger.info(f"Produced message: {message}")
        except KafkaError as e:
            logger.error(f"Failed to produce message: {e}")
            raise

    def close(self):
        """
        Closes the Kafka producer connection gracefully.
        """
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed.")


if __name__ == "__main__":
    # Initialize logger
    logger = setup_logging(LOG_LEVEL)
    logger.info("Kafka producer starting...")

    try:
        # Create Kafka producer
        producer = CustomKafkaProducer(KAFKA_SERVER)

        # Continuously generate and send log data to Kafka
        while True:
            log_data = generate_log_data()  # Generate a new log data record
            producer.publish_message(log_data)  # Produce the log data to the Kafka topic
            time.sleep(PRODUCER_INTERVAL)  # Wait for a specified interval before producing the next message

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        # Ensure the producer connection is closed on exit
        if 'producer' in locals():
            producer.close()
