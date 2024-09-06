import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, unix_timestamp, avg, count, window, to_timestamp, broadcast, round
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)

from config import config


def read_data_from_kafka(spark):
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
            to_timestamp(col("start_timestamp"), "dd-MM-YYYY HH:mm:ss")
        )
        .withColumn(
            "end_timestamp",
            to_timestamp(col("end_timestamp"), "dd-MM-YYYY HH:mm:ss")
        )
    )


def process_data(df, broadcast_campaigns_df):
    df_with_duration = df.withColumn(
        "view_duration",
        unix_timestamp(col("end_timestamp")) - unix_timestamp(col("start_timestamp")),
    )

    # Perform the join with broadcast
    joined_df = df_with_duration.join(
        broadcast_campaigns_df, on="campaign_id", how="left"
    )

    joined_df.printSchema()

    return (
        joined_df.withWatermark("end_timestamp", "10 seconds")
        .groupBy(col("campaign_id"), window(col("end_timestamp"), "1 minute").alias("minute_window"))
        .agg(
            round(avg("view_duration"), 2).alias("avg_duration"),
            count("view_id").alias("total_count"),
        ).withColumn("minute_timestamp", col("minute_window.start"))
    )


def write_data_to_parquet(df, broadcast_campaigns_df):
    final_df = df.join(broadcast_campaigns_df, on="campaign_id", how="left").select(
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


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("BigDataDivasReportGenerator")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2"
        )
        .getOrCreate()
    )


def get_logger() -> logging.Logger:
    logger = logging.getLogger("BigDataDivas")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def main():
    spark = get_spark_session()
    # logger = get_logger()

    # Read campaigns.csv and broadcast it
    campaigns_df = spark.read.csv(
        config["campaigns_csv_path"], header=True, inferSchema=True
    )
    broadcast_campaigns_df = broadcast(campaigns_df)

    # Read Kafka topic
    df = read_data_from_kafka(spark)
    df.printSchema()

    processed_df = process_data(df, broadcast_campaigns_df)
    processed_df.printSchema()

    processed_df.writeStream.format("console").outputMode("append").option(
        "truncate", False
    ).trigger(processingTime="15 seconds").start().awaitTermination()

    # write_data_to_parquet(processed_df, broadcast_campaigns_df)


if __name__ == "__main__":
    main()
