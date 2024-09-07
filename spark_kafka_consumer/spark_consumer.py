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
    broadcast, date_format
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)
from config import config
from utils import get_spark_session, setup_logging


class ViewLogProcessor:
    def __init__(self, spark: SparkSession, logger: Logger):
        """
        Initializes the ViewLogProcessor class with a Spark session.

        Parameters:
            spark (SparkSession): The active Spark session.
        """
        self.spark = spark
        self.logger = logger

    def read_data_from_kafka(self) -> DataFrame:
        """
        Reads streaming data from Kafka and parses it according to the schema.

        Returns:
            DataFrame: A streaming DataFrame containing the parsed view log data.
        """
        try:
            view_log_schema = StructType([
                StructField("view_id", StringType(), True),
                StructField("start_timestamp", TimestampType(), True),
                StructField("end_timestamp", TimestampType(), True),
                StructField("banner_id", IntegerType(), True),
                StructField("campaign_id", IntegerType(), True),
            ])

            df = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", config["KAFKA_SERVER"])
                .option("subscribe", config["TOPIC_NAME"])
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), view_log_schema).alias("view_log"))
                .select("view_log.*")
            )
            self.logger.info("Successfully read data from Kafka.")
            return df

        except Exception as e:
            self.logger.error(f"Error reading data from Kafka: {e}")
            raise

    def process_data(self, df: DataFrame) -> DataFrame:
        """
        Processes the streaming DataFrame to compute average view duration and total view count per minute.

        Parameters:
            df (DataFrame): The streaming DataFrame containing view log data.

        Returns:
            DataFrame: The processed DataFrame with aggregated results.
        """
        try:
            return (
                df.withColumn(
                    "view_duration",
                    unix_timestamp(col("end_timestamp")) - unix_timestamp(col("start_timestamp"))
                )
                .withWatermark(
                    "end_timestamp",
                    "10 seconds"
                )
                .groupBy(
                    col("campaign_id"),
                    window(col("end_timestamp"), "1 minute").alias("minute_window")
                )
                .agg(
                    round(avg("view_duration"), 2).alias("avg_duration"),
                    count("view_id").alias("total_count")
                )
                .withColumn(
                    "minute_timestamp",
                    col("minute_window.start")
                )
            )

        except Exception as e:
            self.logger.error(f"Error processing data: {e}")
            raise

    def write_data_to_parquet(self, df: DataFrame, broadcast_campaigns_df: DataFrame):
        """
        Writes the processed DataFrame to Parquet files, partitioned by network_id and minute_timestamp.
        Change join hint as per requirement.
        Parameters:
            df (DataFrame): The processed DataFrame with aggregated results.
            broadcast_campaigns_df (DataFrame): Broadcast DataFrame containing campaign information.
        """
        try:
            final_df = (
                df.join(broadcast(broadcast_campaigns_df), on="campaign_id", how="inner")
                .select(
                    col("campaign_id"),
                    col("network_id"),
                    date_format(col("minute_timestamp"), "yyyy-MM-dd_HH-mm-ss").alias("minute_timestamp"),
                    col("avg_duration"),
                    col("total_count")
                )
            )

            (
                final_df.writeStream
                .outputMode("append")
                .format("parquet")
                .option("checkpointLocation", config["checkpoint_location"])
                .option("path", config["reports_location"])
                .partitionBy("network_id", "minute_timestamp")
                .start()
                .awaitTermination()
            )

        except Exception as e:
            self.logger.error(f"Error writing data to Parquet: {e}")
            raise


def main():
    """
    Main function to initialize the Spark session and logger, read data from Kafka, process the data,
    and write the results to Parquet files.
    """

    try:
        # Initialize Spark session
        spark = get_spark_session()

        # Initialize Spark session
        logger = setup_logging(config["LOG_LEVEL"])

        # Create instance of ViewLogProcessor
        processor = ViewLogProcessor(spark, logger)

        # Read and broadcast the campaign data
        campaigns_df = spark.read.csv(
            config["campaigns_csv_path"], header=True, inferSchema=True
        )
        broadcast_campaigns_df = broadcast(campaigns_df)

        # Read streaming data from Kafka
        source_log_df = processor.read_data_from_kafka()
        source_log_df.printSchema()

        # Process the streaming data
        processed_log_df = processor.process_data(source_log_df)
        processed_log_df.printSchema()

        # Write to file system
        processor.write_data_to_parquet(processed_log_df, broadcast_campaigns_df)

        # processed_df.writeStream\
        #     .format("console")\
        #     .outputMode("append")\
        #     .option("truncate", False)\
        #     .trigger(processingTime="15 seconds")\
        #     .start()\
        #     .awaitTermination()

    except Exception as e:
        logger.error(f"An error occurred in Log Processor: {e}")


if __name__ == "__main__":
    main()
