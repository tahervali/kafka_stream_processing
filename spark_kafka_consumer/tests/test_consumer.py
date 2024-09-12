import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from src.consumer import ViewLogProcessor


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("ViewLogProcessorTest") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def mock_logger():
    return MagicMock()


@pytest.fixture
def mock_config():
    mock_config = MagicMock()
    return mock_config


@pytest.fixture
def mock_processor(spark, mock_logger, mock_config):
    # Mock the ViewLogProcessor initialization
    processor = ViewLogProcessor(spark=spark, logger=mock_logger)
    processor.config = mock_config  # Mock the config

    # Mock the load_and_broadcast_campaigns method to return a sample DataFrame
    campaigns_df = spark.createDataFrame([
        (101, "Campaign 1", "1"),
        (102, "Campaign 2", "2"),
        (103, "Campaign 3", "3")
    ], ["campaign_id", "campaign_name", "network_id"])
    processor.load_and_broadcast_campaigns = MagicMock(return_value=campaigns_df)

    return processor


# Add test functions here..

# Dummy test Added for now
def test_dummy():
    pass


if __name__ == "__main__":
    pytest.main()
