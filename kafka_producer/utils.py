import logging


def setup_logging(log_level=logging.INFO):
    """
    Sets up the logging configuration.

    Args:
        log_level (int): The logging level to use.
    """
    logging.basicConfig(level=log_level)
    logger = logging.getLogger(__name__)
    return logger
