import logging
import sys


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
