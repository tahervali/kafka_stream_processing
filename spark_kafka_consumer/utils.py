import logging
import sys

from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("BigDataDivasReportGenerator")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "gzip")
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
        .config("spark.streaming.kafka.consumer.poll.ms", "500")
        # .config("parquet.block.size", "268435456")
        .config("spark.sql.shuffle.partitions", "30")
        .getOrCreate()
    )


def setup_logging(log_level=logging.INFO):
    """
    Sets up the logging configuration for Dockerized environments.

    Parameters:
        log_level : The logging level to use (default: INFO).
    Returns:
        logging.Logger: Configured logger instance.
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    # Create log formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Set up console logging handler (stdout in Docker)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
