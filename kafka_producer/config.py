import os


class Config:
    """
    Config class for managing application settings by loading values from environment variables.
    Automatically uses default values if environment variables are not set.
    """

    def __init__(self):
        self.KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'broker:9092')
        self.TOPIC_NAME = os.getenv('TOPIC_NAME', 'test_topic')
        self.MAX_RETRIES = self._check_positive(int(os.getenv('MAX_RETRIES', 5)), 'MAX_RETRIES')
        self.RETRY_DELAY = self._check_positive(int(os.getenv('RETRY_DELAY', 3)), 'RETRY_DELAY')  # seconds
        self.PRODUCER_INTERVAL = self._check_positive(int(os.getenv('PRODUCER_INTERVAL', 1)), 'PRODUCER_INTERVAL')
        self.THREAD_COUNT = self._check_positive(int(os.getenv('THREAD_COUNT', 5)), 'THREAD_COUNT')
        self.BATCH_SIZE = self._check_positive(int(os.getenv('BATCH_SIZE', 30)), 'BATCH_SIZE')
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

        # Perform further validation if needed
        self._validate_log_level()

    @staticmethod
    def _check_positive(value: int, field_name: str) -> int:
        """
        Checks if a given value is positive.

        Parameters:
            value (int or float): The value to check.
            field_name (str): The name of the field being checked.

        Returns:
            int or float: The validated value if it's positive.

        Raises:
            ValueError: If the value is not positive.
        """
        if value < 0:
            raise ValueError(f"{field_name} must be positive, got {value}")
        return value

    def _validate_log_level(self):
        """
        Additional validation for configuration fields.
        """
        valid_log_levels = {"DEBUG", "INFO", "WARN", "ERROR"}
        if self.LOG_LEVEL.upper() not in valid_log_levels:
            raise ValueError(f"Invalid LOG_LEVEL: {self.LOG_LEVEL}. Must be one of {valid_log_levels}")


# Instantiate the configuration class (this reads from environment variables or uses defaults)
config = Config()
print(f" THREAD_COUNT is: {config.THREAD_COUNT}")
print(f" BATCH_SIZE is: {config.BATCH_SIZE}")
print(f" LOG_LEVEL is: {config.LOG_LEVEL}")
