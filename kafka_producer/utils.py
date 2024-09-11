import logging
import random
import sys
import uuid
from datetime import timedelta, datetime


def setup_logging(log_level=logging.INFO):
    """
    Sets up the logging configuration for Dockerized environments.

    Parameters:
        log_level (int): The logging level to use (default: INFO).
    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    # Create log formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Set up console logging handler (stdout in Docker)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


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
