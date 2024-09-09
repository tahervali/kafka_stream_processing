from pydantic import BaseModel, Field, ConfigDict, field_validator


class Config(BaseModel):
    """
    Config class for managing application settings using Pydantic's BaseModel in V2.
    Automatically loads values from environment variables or defaults specified via Field declarations.

    Attributes:
        KAFKA_SERVER (str): The Kafka server address, defaulting to "broker:9092".
        TOPIC_NAME (str): Kafka topic to produce/consume messages, default "test_topic".
        MAX_RETRIES (int): Maximum number of retries for failed operations, default 5.
        RETRY_DELAY (int): Delay between retries in seconds, default 5 seconds.
        PRODUCER_INTERVAL (float): Interval between producer messages in seconds, default 1.0.
        LOG_LEVEL (str): Logging level for the application, default "INFO".
    """

    KAFKA_SERVER: str = Field(default="broker:9092", env="KAFKA_SERVER")
    TOPIC_NAME: str = Field(default="test_topic", env="TOPIC_NAME")
    MAX_RETRIES: int = Field(default=5, env="MAX_RETRIES")
    RETRY_DELAY: int = Field(default=5, env="RETRY_DELAY")  # seconds
    PRODUCER_INTERVAL: int = Field(default=1, env="PRODUCER_INTERVAL")
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")

    model_config = ConfigDict(env_file=".env", validate_assignment=True)

    @field_validator("MAX_RETRIES", "RETRY_DELAY", mode="before")
    def check_positive(cls, value):
        """
        Validator to ensure that retry-related configurations (MAX_RETRIES, RETRY_DELAY) are positive integers.
        Raises:
            ValueError: If the value is less than 0.
        """
        if value < 0:
            raise ValueError("Value must be positive")
        return value


# Instantiate the configuration class (this reads from environment variables or uses defaults)
config = Config()
