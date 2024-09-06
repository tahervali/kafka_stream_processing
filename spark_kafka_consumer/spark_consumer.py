# from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    unix_timestamp,
    avg,
    count,
    window,
    to_timestamp,
    broadcast,
    round,
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


def read_data_from_kafka(spark):
    """
    Reads streaming data from Kafka and parses it according to the schema.

    Args:
        spark (SparkSession): The active Spark session.

    Returns:
        DataFrame: A streaming DataFrame containing the parsed view log data.
    """
    view_log_schema = StructType(
        [
            StructField("view_id", StringType(), True),
            StructField("start_timestamp", TimestampType(), True),
            StructField("end_timestamp", TimestampType(), True),
            StructField("banner_id", IntegerType(), True),
            StructField("campaign_id", IntegerType(), True),
        ]
    )

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config["KAFKA_SERVER"])
        .option("subscribe", config["TOPIC_NAME"])
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), view_log_schema).alias("view_log"))
        .select("view_log.*")
        .withColumn(
            "start_timestamp",
            to_timestamp(col("start_timestamp"), "dd-MM-YYYY HH:mm:ss"),
        )
        .withColumn(
            "end_timestamp", to_timestamp(col("end_timestamp"), "dd-MM-YYYY HH:mm:ss")
        )
    )


def process_data(df):
    df_with_duration = df.withColumn(
        "view_duration",
        unix_timestamp(col("end_timestamp")) - unix_timestamp(col("start_timestamp")),
    )

    return (
        df_with_duration.withWatermark("end_timestamp", "10 seconds")
        .groupBy(
            col("campaign_id"),
            window(col("end_timestamp"), "1 minute").alias("minute_window"),
        )
        .agg(
            round(avg("view_duration"), 2).alias("avg_duration"),
            count("view_id").alias("total_count"),
        )
        .withColumn("minute_timestamp", col("minute_window.start"))
    )


def write_data_to_parquet(df, broadcast_campaigns_df):
    """
    Writes the processed DataFrame to Parquet files, partitioned by network_id and minute_timestamp.

    Args:
        df (DataFrame): The processed DataFrame with aggregated results.
        broadcast_campaigns_df (DataFrame): Broadcasted DataFrame containing campaign information.
    """
    final_df = df.join(
        broadcast(broadcast_campaigns_df), on="campaign_id", how="inner"
    ).select(
        col("campaign_id"),
        col("network_id"),
        col("minute_timestamp"),
        col("avg_duration"),
        col("total_count"),
    )

    final_df.writeStream.outputMode("append").format("parquet").option(
        "checkpointLocation", config["checkpoint_location"]
    ).option("path", config["reports_location"]).partitionBy(
        "network_id", "minute_timestamp"
    ).start().awaitTermination()


def main():
    """
    Main function to initialize the Spark session, read data from Kafka, process the data,
    and write the results to Parquet files.
    """
    # Initialize logger
    logger = setup_logging(config["LOG_LEVEL"])
    logger.info("Consumer initialize...")

    # Initialize Spark session
    spark = get_spark_session()

    # Read and broadcast the campaign data
    campaigns_df = spark.read.csv(
        config["campaigns_csv_path"], header=True, inferSchema=True
    )
    print("Showing from mount --->")
    campaigns_df.show(5)
    broadcast_campaigns_df = broadcast(campaigns_df)

    # Read streaming data from Kafka
    df = read_data_from_kafka(spark)
    df.printSchema()

    # Process the streaming data
    processed_df = process_data(df)
    processed_df.printSchema()

    # processed_df.writeStream\
    #     .format("console")\
    #     .outputMode("append")\
    #     .option("truncate", False)\
    #     .trigger(processingTime="15 seconds")\
    #     .start()\
    #     .awaitTermination()

    write_data_to_parquet(processed_df, broadcast_campaigns_df)


if __name__ == "__main__":
    main()
