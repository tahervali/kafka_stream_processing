import logging
import sys

from pyspark.sql import SparkSession


def get_spark_session():
    """
    Initializes and returns a configured SparkSession for the report generation application.

    The SparkSession is configured with specific settings for Kafka integration, data serialization,
    Parquet compression, streaming options, and shuffle partitions. These settings are tailored for
    efficient processing in a big data environment, particularly for applications that involve streaming
    data from Kafka and writing results to Parquet.

    Configurations:
        - spark.jars.packages: Adds the Kafka integration package for Spark.
        - spark.serializer: Uses KryoSerializer for efficient serialization.
        - spark.sql.parquet.compression.codec: Compresses Parquet files using GZIP.
        - spark.sql.streaming.noDataMicroBatches.enabled: Disables micro-batches when there's no data.
        - spark.streaming.kafka.consumer.poll.ms: Sets the Kafka consumer poll interval to 500ms.
        - spark.sql.shuffle.partitions: Sets the number of shuffle partitions to 30 for optimization.

    Returns:
        SparkSession: A configured SparkSession instance.
    """
    return (
        SparkSession.builder.appName("BigDataDivasReportGenerator")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "gzip")
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
        .config("spark.streaming.kafka.consumer.poll.ms", "500")
        .config("spark.sql.shuffle.partitions", "30")
        .getOrCreate()
    )


def setup_logging(log_level=logging.INFO):
    """
    Sets up the logging configuration for Dockerized environments.

    Parameters:
        log_level (str or int): The logging level to use. Can be either an integer
        (e.g., logging.INFO) or a string (e.g., "INFO"). Default is logging.INFO.
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
