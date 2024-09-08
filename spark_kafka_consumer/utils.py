import logging
import sys

from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("BigDataDivasReportGenerator")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2"
        )
        .config("spark.sql.parquet.compression.codec", "gzip")
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
        .config("spark.streaming.kafka.consumer.poll.ms", "500")
        # .config("parquet.block.size", "268435456")
        .config("spark.sql.shuffle.partitions", "30")
        .getOrCreate()
    )


def setup_logging(
    log_level=logging.INFO, log_to_file=False, log_file_path="/var/log/app/app.log"
):
    """
    Sets up the logging configuration for Dockerized environments.

    Args:
        log_level (int): The logging level to use (default: INFO).
        log_to_file (bool): Whether to log to a file (default: False).
        log_file_path (str): The file path to log to, if log_to_file is True (default: "/var/log/app/app.log").

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

    # Set up file logging handler if enabled (logs will be written inside the Docker container)
    if log_to_file:
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
