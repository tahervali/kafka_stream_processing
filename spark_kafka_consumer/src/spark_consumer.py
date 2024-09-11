from logging import Logger

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    unix_timestamp,
    avg,
    count,
    window,
    round,
    broadcast,
    date_format,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)

from src.config import config
from src.utils import get_spark_session, setup_logging


class ViewLogProcessor:
    """
    A class to process view logs from Kafka, aggregate the data, and write it to Parquet files.

    This class handles the entire ETL pipeline: reading from Kafka, processing the data,
    and writing the results to Parquet files. It also manages the broadcasting of campaign data
    for efficient joins.

    Attributes:
        spark (SparkSession): The active Spark session.
        logger (Logger): Logger for recording processing information and errors.
        window_duration (str): The duration of the window for aggregating data.
        watermark_delay (str): The maximum duration of allowed late data.
        processing_time (str): The processing time interval for the streaming query.
        view_log_schema (StructType): The schema for the view log data from Kafka.
        broadcast_campaigns_df (DataFrame): DataFrame containing campaign information.
    """

    def __init__(self, spark: SparkSession, logger: Logger):
        """
        Initialize the ViewLogProcessor with a SparkSession, logger, and optional parameters.

        Args:
            spark (SparkSession): The active Spark session.
            logger (Logger): Logger for recording processing information and errors.
        """
        self.spark = spark
        self.logger = logger
        self.window_duration = config.WINDOW_DURATION
        self.watermark_delay = config.WATERMARK_DELAY
        self.processing_time = config.PROCESSING_TIME
        self.view_log_schema = StructType(
            [
                StructField("view_id", StringType(), True),
                StructField("start_timestamp", TimestampType(), True),
                StructField("end_timestamp", TimestampType(), True),
                StructField("banner_id", IntegerType(), True),
                StructField("campaign_id", IntegerType(), True),
            ]
        )
        self.broadcast_campaigns_df = self.load_and_broadcast_campaigns()

    def load_and_broadcast_campaigns(self) -> DataFrame:
        """
        Load campaign data from CSV and broadcast it for efficient joins.

        Returns:
            DataFrame: DataFrame containing campaign information.
        """
        try:
            campaigns_df = self.spark.read.csv(
                config.CAMPAIGNS_CSV_PATH, header=True, inferSchema=True
            ).repartition("campaign_id")
            return broadcast(campaigns_df)
        except Exception as e:
            self.logger.error(f"Error loading and broadcasting campaigns data: {e}")
            raise

    def read_data_from_kafka(self) -> DataFrame:
        """
        Read streaming data from Kafka and parse it according to the view log schema.

        Returns:
            DataFrame: A streaming DataFrame containing the parsed view log data.
        """
        try:
            source_df = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", config.KAFKA_SERVER)
                .option("subscribe", config.TOPIC_NAME)
                .option("failOnDataLoss", "false")
                .load()
                .select(
                    from_json(col("value").cast("string"), self.view_log_schema).alias(
                        "view_log"
                    )
                )
                .select("view_log.*")
            )
            self.logger.info("Successfully read data from Kafka.")
            return source_df
        except Exception as e:
            self.logger.error(f"Error reading data from Kafka: {e}")
            raise

    def process_data(self, df: DataFrame) -> DataFrame:
        """
        Process the streaming DataFrame to compute average view duration and total view count per window,
        and join with campaign data.

        Args:
            df (DataFrame): The streaming DataFrame containing view log data.

        Returns:
            DataFrame: The processed DataFrame with aggregated results and campaign information.
        """
        try:
            processed_df = (
                df.dropDuplicates(["view_id"])
                .withColumn(
                    "view_duration",
                    unix_timestamp("end_timestamp") - unix_timestamp("start_timestamp"),
                )
                .withWatermark("end_timestamp", self.watermark_delay)
                .groupBy(
                    "campaign_id",
                    window("end_timestamp", self.window_duration).alias("time_window"),
                )
                .agg(
                    round(avg("view_duration"), 2).alias("avg_duration"),
                    count("view_id").alias("total_count"),
                )
                .select(
                    "campaign_id",
                    col("time_window.start").alias("minute_timestamp"),
                    "avg_duration",
                    "total_count",
                )
            )

            # Join with campaign data
            return processed_df.join(
                self.broadcast_campaigns_df, on="campaign_id", how="inner"
            ).select(
                "campaign_id",
                "network_id",
                date_format("minute_timestamp", "yyyy-MM-dd_HH-mm-ss").alias(
                    "minute_timestamp"
                ),
                "avg_duration",
                "total_count",
            )
        except Exception as e:
            self.logger.error(f"Error processing data: {e}")
            raise

    def write_data_to_parquet(self, df: DataFrame):
        """
        Write the processed DataFrame to Parquet files, partitioned by network_id and minute_timestamp.

        This method also starts the streaming query and waits for its termination.

        Args:
            df (DataFrame): The processed DataFrame with aggregated results and campaign information.

        Raises:
            Exception: If there's an error writing data to Parquet files or managing the streaming query.
        """
        try:
            query = (
                df.writeStream.outputMode("append")
                .format("parquet")
                .trigger(processingTime=self.processing_time)
                .option("checkpointLocation", config.CHECKPOINT_LOCATION)
                .option("path", config.REPORTS_LOCATION)
                .partitionBy("network_id", "minute_timestamp")
                .start()
            )

            self.logger.info(
                f"Streaming query started with processing time {self.processing_time}. "
                f"Waiting for termination..."
            )
            query.awaitTermination()

        except Exception as e:
            self.logger.error(
                f"Error writing data to Parquet or managing streaming query: {e}"
            )
            raise


def main():
    """
    Main function to initialize the Spark session, logger, and ViewLogProcessor.
    It orchestrates the entire ETL process: reading from Kafka, processing data, and writing to Parquet.
    """

    # Initialize logger
    logger = setup_logging(config.LOG_LEVEL)
    logger.info(f"KAFKA_SERVER: {config.KAFKA_SERVER}")
    logger.info(f"LOG_LEVEL: {config.LOG_LEVEL}")
    logger.info(f"TEST_VALUE: {config.TEST_VALUE}")

    try:
        # Initialize Spark session
        spark = get_spark_session()

        # Initialize processor with custom parameters
        processor = ViewLogProcessor(spark, logger)

        # Read streaming data from Kafka
        source_log_df = processor.read_data_from_kafka()

        # Process the streaming data and join with campaign data
        processed_log_df = processor.process_data(source_log_df)

        # Write processed data to Parquet files and await termination
        processor.write_data_to_parquet(processed_log_df)

    except Exception as e:
        logger.error(f"An error occurred in Log Processor: {e}")


if __name__ == "__main__":
    main()
