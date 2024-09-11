import pytest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock, patch

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.consumer import ViewLogProcessor


@pytest.fixture(scope="function")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("test") \
        .getOrCreate()


@pytest.fixture(scope="function")
def logger():
    return MagicMock()


@pytest.fixture(scope="function")
def processor(spark: SparkSession, logger: MagicMock):
    with patch('src.config.config', MagicMock()):
        return ViewLogProcessor(spark, logger)


def test_load_and_broadcast_campaigns(processor: ViewLogProcessor, spark: SparkSession):
    # Create a mock DataFrame
    schema = StructType([
        StructField("campaign_id", IntegerType(), True),
        StructField("network_id", IntegerType(), True)
    ])

    data = [
        (101, 1),
        (201, 2),
    ]

    campaigns_df = spark.createDataFrame(data, schema)

    # Mock the read.csv method to return our mock DataFrame
    with patch.object(processor.spark.read, 'csv', return_value=campaigns_df):
        broadcast_df = processor.load_and_broadcast_campaigns()

    assert broadcast_df.count() == len(data)
    assert set(row.campaign_id for row in broadcast_df.collect()) == {1, 2}