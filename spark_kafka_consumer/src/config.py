import os

# from dotenv import load_dotenv

# # Load environment variables from .env file
# load_dotenv()


class Config:
    def __init__(self):
        # Load environment variables with defaults
        self.KAFKA_SERVER = os.getenv("KAFKA_SERVER", "broker:9092")
        self.TOPIC_NAME = os.getenv("TOPIC_NAME", "test_topic")
        self.TEST_VALUE = os.getenv("TEST_VALUE", "Got it")
        print(f"TEST VALUE-->{self.TEST_VALUE}")
        self.LOG_LEVEL = os.getenv("LOG_LEVEL")
        print(f"setting level--->{self.LOG_LEVEL}")

        self.WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
        self.WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 seconds")
        self.PROCESSING_TIME = os.getenv("PROCESSING_TIME", "0 seconds")
        self.BASE_DATA_LOC_CONTAINER = os.getenv("BASE_DATA_LOC_CONTAINER", "/app/data")
        self.CAMPAIGNS_CSV_PATH = (
            self.BASE_DATA_LOC_CONTAINER + "/input_data/campaigns.csv"
        )
        self.REPORTS_LOCATION = self.BASE_DATA_LOC_CONTAINER + "/output_data/"
        self.CHECKPOINT_LOCATION = self.REPORTS_LOCATION + "_saveloc"

        # self.CAMPAIGNS_CSV_PATH = os.getenv(
        #     "CAMPAIGNS_CSV_PATH", "/app/input_data/campaigns.csv"
        # )
        # self.CHECKPOINT_LOCATION = os.getenv(
        #     "CHECKPOINT_LOCATION", "/app/data/_saveloc"
        # )
        # self.REPORTS_LOCATION = os.getenv("REPORTS_LOCATION", "/app/data/")

        # Validate fields
        self._validate()

    def _validate(self):
        if (
            "minute" not in self.WINDOW_DURATION
            and "seconds" not in self.WINDOW_DURATION
        ):
            raise ValueError(
                f"Invalid time format for WINDOW_DURATION: {self.WINDOW_DURATION}."
                f" Must contain 'minute' or 'seconds'."
            )
        if (
            "minute" not in self.WATERMARK_DELAY
            and "seconds" not in self.WATERMARK_DELAY
        ):
            raise ValueError(
                f"Invalid time format for WATERMARK_DELAY: {self.WATERMARK_DELAY}. "
                f"Must contain 'minute' or 'seconds'."
            )
        if (
            "minute" not in self.PROCESSING_TIME
            and "seconds" not in self.PROCESSING_TIME
        ):
            raise ValueError(
                f"Invalid time format for PROCESSING_TIME: {self.PROCESSING_TIME}. "
                f"Must contain 'minute' or 'seconds'."
            )

        for path in [
            self.CAMPAIGNS_CSV_PATH,
            self.CHECKPOINT_LOCATION,
            self.REPORTS_LOCATION,
        ]:
            if not path:
                raise ValueError(f"Path cannot be empty: {path}")

        # valid_levels = {"DEBUG", "INFO", "WARN", "ERROR"}
        # if self.LOG_LEVEL.upper() not in valid_levels:
        #     raise ValueError(
        #         f"Invalid LOG_LEVEL: {self.LOG_LEVEL}. Must be one of {valid_levels}."
        #     )


# Instantiate the configuration class (this reads from .env or uses defaults)
config = Config()
print(f" KAFKA_SERVER is: {config.KAFKA_SERVER}")
print(f" TOPIC_NAME is: {config.TOPIC_NAME}")
print(f" TEST_VALUE is: {config.TEST_VALUE}")
print(f" TEST_VALUE os env is: {os.getenv('TEST_VALUE')}")
