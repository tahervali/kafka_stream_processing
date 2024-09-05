import json
import random
import time
import uuid
from datetime import timedelta

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import KAFKA_SERVER, TOPIC_NAME, MAX_RETRIES, RETRY_DELAY, PRODUCER_INTERVAL
from utils import setup_logging

# Initialize Faker instance
fake = Faker()

# Initialize logger
logger = setup_logging()


def generate_log_data():
    """
    Generates a fake log record according to the specified schema.

    Returns:
        dict: A dictionary containing the log data with the following fields:
            - view_id (str): Unique ID of the record
            - start_timestamp (str): Timestamp when the view started
            - end_timestamp (str): Timestamp when the view ended (and pushed to Kafka)
            - banner_id (int): The banner ID
            - campaign_id (int): The campaign ID
    """
    view_id = str(uuid.uuid4())  # Generate a unique ID for the record
    start_timestamp = fake.date_time_this_year()  # Generate a random start time in the current year
    end_timestamp = start_timestamp + timedelta(seconds=random.randint(1, 300))  # Generate end time after start time
    banner_id = random.randint(1, 1000)  # Generate a random banner ID
    campaign_id = random.randint(1, 200)  # Generate a random campaign ID

    # Return the generated data as a dictionary
    return {
        "view_id": view_id,
        "start_timestamp": start_timestamp.isoformat(),
        "end_timestamp": end_timestamp.isoformat(),
        "banner_id": banner_id,
        "campaign_id": campaign_id
    }


class CustomKafkaProducer:
    def __init__(self, bootstrap_servers):
        """
        Initializes the CustomKafkaProducer with the given Kafka bootstrap servers.

        Args:
            bootstrap_servers (str): Address of the Kafka bootstrap server(s).
        """
        self.producer = None
        self.bootstrap_servers = bootstrap_servers
        self.connect()

    def connect(self):
        """
        Attempts to connect to the Kafka broker with retry logic.
        If the connection fails after MAX_RETRIES, an exception is raised.
        """
        retries = 0
        while retries < MAX_RETRIES:
            try:
                # Initialize the Kafka producer with JSON serialization
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

    def publish_message(self, message):
        """
        Produces a message to the Kafka topic.

        Args:
            message (dict): The message to be sent to the Kafka topic.
        """
        try:
            # Send the message to the Kafka topic
            self.producer.send(TOPIC_NAME, value=message)
            self.producer.flush()  # Ensure all messages are sent
            logger.info(f"Produced message: {message}")
        except KafkaError as e:
            logger.error(f"Failed to produce message: {e}")


if __name__ == "__main__":
    producer = CustomKafkaProducer(KAFKA_SERVER)

    # Continuously generate and send log data to Kafka
    while True:
        # Generate a new log data record
        log_data = generate_log_data()

        # Produce the generated log data to the Kafka topic
        producer.publish_message(log_data)

        # Wait for a specified interval before producing the next message
        time.sleep(PRODUCER_INTERVAL)
