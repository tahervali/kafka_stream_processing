import os


class Config:
    """
    Config class for managing application settings by loading values from environment variables.
    Automatically falls back to default values if environment variables are not set.

    Attributes:
        KAFKA_SERVER (str): Address of the Kafka bootstrap server(s).
        TOPIC_NAME (str): Kafka topic name for publishing messages.
        MAX_RETRIES (int): Maximum number of retry attempts for failed Kafka connections.
        RETRY_DELAY (int): Delay in seconds between retries for Kafka connection failures.
        PRODUCER_INTERVAL (int): Interval in seconds between producing batches of messages.
        THREAD_COUNT (int): Number of threads to use for producing messages in parallel.
        BATCH_SIZE (int): Number of messages to produce in each batch.
        LOG_LEVEL (str): Logging level for the application.
    """

    def __init__(self):
        """
        Initializes the Config class by loading environment variables or using default values.
        """
        self.KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'broker:9092')
        self.TOPIC_NAME = os.getenv('TOPIC_NAME', 'view_log')
        self.MAX_RETRIES = self._check_positive(int(os.getenv('MAX_RETRIES', 5)), 'MAX_RETRIES')
        self.RETRY_DELAY = self._check_positive(int(os.getenv('RETRY_DELAY', 3)), 'RETRY_DELAY')
        self.PRODUCER_INTERVAL = self._check_positive(int(os.getenv('PRODUCER_INTERVAL', 1)), 'PRODUCER_INTERVAL')
        self.THREAD_COUNT = self._check_positive(int(os.getenv('THREAD_COUNT', 5)), 'THREAD_COUNT')
        self.BATCH_SIZE = self._check_positive(int(os.getenv('BATCH_SIZE', 30)), 'BATCH_SIZE')
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

        # Perform validation for log level
        self._validate_log_level()

    @staticmethod
    def _check_positive(value: int, field_name: str) -> int:
        """
        Ensures that the given value is positive.

        Parameters:
            value (int): The value to check.
            field_name (str): The name of the configuration field being validated.

        Returns:
            int: The validated value if it is positive.
        """
        if value < 0:
            raise ValueError(f"{field_name} must be positive, got {value}")
        return value

    def _validate_log_level(self):
        """
        Validates the LOG_LEVEL configuration field to ensure it's set to an appropriate value.

        Raises:
            ValueError: If the LOG_LEVEL is not a valid logging level.
        """
        valid_log_levels = {"DEBUG", "INFO", "WARN", "ERROR"}
        if self.LOG_LEVEL.upper() not in valid_log_levels:
            raise ValueError(f"Invalid LOG_LEVEL: {self.LOG_LEVEL}. Must be one of {valid_log_levels}")


# Instantiate the configuration class (this reads from environment variables or uses defaults)
config = Config()
