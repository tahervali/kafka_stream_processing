from pydantic import BaseModel, Field, ConfigDict, field_validator


class Config(BaseModel):
    """
    Config class for managing application settings using Pydantic's BaseModel.
    Automatically loads values from environment variables or defaults specified via Field declarations.
    Also, Validate the fields using field_validator.
    """

    KAFKA_SERVER: str = Field(default="broker:9092", env="KAFKA_SERVER")
    TOPIC_NAME: str = Field(default="test_topic", env="TOPIC_NAME")
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")
    WINDOW_DURATION: str = Field(default="1 minute", env="WINDOW_DURATION")
    WATERMARK_DELAY: str = Field(default="10 seconds", env="WATERMARK_DELAY")
    PROCESSING_TIME: str = Field(default="0 seconds", env="PROCESSING_TIME")
    CAMPAIGNS_CSV_PATH: str = Field(default="/app/input_data/campaigns.csv")
    CHECKPOINT_LOCATION: str = Field(default="/app/data/_saveloc")
    REPORTS_LOCATION: str = Field(default="/app/data/")

    model_config = ConfigDict(env_file=".env", validate_assignment=True)

    # Validator to check the validity of the time fields
    @field_validator("WINDOW_DURATION", "WATERMARK_DELAY", "PROCESSING_TIME", mode="before")
    def validate_time_format(cls, value: str) -> str:
        """
        Validates the format of the time duration fields to ensure they follow proper format.
        Example formats: "1 minute", "30 seconds", "0 seconds".
        """
        if "minute" not in value and "seconds" not in value:
            raise ValueError(f"Invalid time format for {value}. Must contain 'minute' or 'seconds'.")
        return value

    # Validator to check if paths are non-empty strings
    @field_validator("CAMPAIGNS_CSV_PATH", "CHECKPOINT_LOCATION", "REPORTS_LOCATION", mode="before")
    def validate_paths(cls, value: str) -> str:
        """
        Validates that path fields are not empty.
        Raises:
            ValueError: If the path is an empty string.
        """
        if not value or not isinstance(value, str):
            raise ValueError(f"Invalid path: {value}. Must be a non-empty string.")
        return value

    @field_validator("LOG_LEVEL", mode="before")
    def validate_log_level(cls, value: str) -> str:
        """
        Validates the log level to ensure it's a valid logging level.
        """
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if value.upper() not in valid_levels:
            raise ValueError(f"Invalid LOG_LEVEL: {value}. Must be one of {valid_levels}.")
        return value.upper()


# Instantiate the configuration class (this reads from environment variables or uses defaults)
config = Config()
