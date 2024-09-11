import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.consumer import ViewLogProcessor


# Define mock configuration
class MockConfig:
    WINDOW_DURATION = "10 minutes"
    WATERMARK_DELAY = "5 minutes"
    PROCESSING_TIME = "1 minute"
    CAMPAIGNS_CSV_PATH = "mock_path"  # Not used in the test
    KAFKA_SERVER = "localhost:9092"
    TOPIC_NAME = "mock_topic"
    CHECKPOINT_LOCATION = "mock_checkpoint"
    REPORTS_LOCATION = "mock_reports"


# Mock logger
class MockLogger:
    def info(self, msg: str):
        pass

    def error(self, msg: str):
        pass


@pytest.fixture(scope="function")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("test") \
        .getOrCreate()


@pytest.fixture(scope="function")
def logger():
    return MockLogger()


@pytest.fixture(scope="function")
def processor(spark: SparkSession, logger: MockLogger):
    with patch('src.config.config', MockConfig):
        return ViewLogProcessor(spark, logger)


def test_load_and_broadcast_campaigns(processor: ViewLogProcessor, spark: SparkSession):
    # Create a mock DataFrame
    schema = StructType([
        StructField("campaign_id", IntegerType(), True),
        StructField("network_id", StringType(), True)
    ])

    data = [
        (1, "NetworkA"),
        (2, "NetworkB"),
    ]

    campaigns_df = spark.createDataFrame(data, schema)

    # Mock the read.csv method to return our mock DataFrame
    with patch.object(processor.spark.read, 'csv', return_value=campaigns_df) as mock_csv:
        broadcast_df = processor.load_and_broadcast_campaigns()

    # Assert that the DataFrame is as expected
    assert broadcast_df.count() == len(data)
    assert set(row.campaign_id for row in broadcast_df.collect()) == {1, 2}

    # Verify the read.csv method was called with the correct path
    mock_csv.assert_called_once_with(MockConfig.CAMPAIGNS_CSV_PATH, header=True, inferSchema=True)

