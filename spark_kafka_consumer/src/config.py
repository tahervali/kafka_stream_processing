import os


class Config:
    """
    Configuration class for managing application settings by loading environment variables.
    It applies default values where environment variables are not set and validates critical fields.

    Attributes:
        KAFKA_SERVER (str): The Kafka server address. Default is 'broker:9092'.
        TOPIC_NAME (str): The Kafka topic name. Default is 'test_topic'.
        LOG_LEVEL (str): The logging level (DEBUG, INFO, WARN, ERROR).
        WINDOW_DURATION (str): Duration of the window for stream processing. Default is '1 minute'.
        WATERMARK_DELAY (str): Maximum allowed lateness for event-time processing. Default is '10 seconds'.
        PROCESSING_TIME (str): The interval for processing in streaming jobs. Default is '0 seconds'.
        BASE_DATA_LOC_CONTAINER (str): Base directory for input and output data inside the container.
        CAMPAIGNS_CSV_PATH (str): Path to the campaigns CSV file.
        REPORTS_LOCATION (str): Directory where reports are saved.
        CHECKPOINT_LOCATION (str): Directory for saving checkpoints during stream processing.
    """

    def __init__(self):
        """
        Initializes the Config object and loads environment variables with defaults if not provided.
        Calls _validate() to ensure correctness of certain attributes.
        """
        self.KAFKA_SERVER = os.getenv("KAFKA_SERVER", "broker:9092")
        self.TOPIC_NAME = os.getenv("TOPIC_NAME", "view_log")
        self.LOG_LEVEL = os.getenv("LOG_LEVEL")
        self.WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
        self.WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 seconds")
        self.PROCESSING_TIME = os.getenv("PROCESSING_TIME", "0 seconds")
        self.BASE_DATA_LOC_CONTAINER = os.getenv("BASE_DATA_LOC_CONTAINER", "/app/data")
        self.CAMPAIGNS_CSV_PATH = (self.BASE_DATA_LOC_CONTAINER + "/input_data/campaigns.csv")
        self.REPORTS_LOCATION = self.BASE_DATA_LOC_CONTAINER + "/output_data/"
        self.CHECKPOINT_LOCATION = self.REPORTS_LOCATION + "_saveloc"

        # Validate fields
        self._validate()

    def _validate(self):
        """
        Validates certain fields to ensure that they are formatted or configured correctly.

        Raises:
            ValueError: If any time-based field is not in the correct format,
                        or if any directory path is empty or invalid.
        """
        if not ("minute" in self.WINDOW_DURATION or "seconds" in self.WINDOW_DURATION):
            raise ValueError(
                f"Invalid time format for WINDOW_DURATION: {self.WINDOW_DURATION}. "
                f"Must contain 'minute' or 'seconds'."
            )
        if not ("minute" in self.WATERMARK_DELAY or "seconds" in self.WATERMARK_DELAY):
            raise ValueError(
                f"Invalid time format for WATERMARK_DELAY: {self.WATERMARK_DELAY}. "
                f"Must contain 'minute' or 'seconds'."
            )
        if not ("minute" in self.PROCESSING_TIME or "seconds" in self.PROCESSING_TIME):
            raise ValueError(
                f"Invalid time format for PROCESSING_TIME: {self.PROCESSING_TIME}. "
                f"Must contain 'minute' or 'seconds'."
            )

        for path in [self.CAMPAIGNS_CSV_PATH, self.CHECKPOINT_LOCATION, self.REPORTS_LOCATION]:
            if not path:
                raise ValueError(f"Path cannot be empty: {path}")

        valid_levels = {"DEBUG", "INFO", "WARN", "ERROR"}
        if self.LOG_LEVEL and self.LOG_LEVEL.upper() not in valid_levels:
            raise ValueError(
                f"Invalid LOG_LEVEL: {self.LOG_LEVEL}. Must be one of {valid_levels}."
            )


# Instantiate the configuration class (this reads from .env or uses defaults)
config = Config()
