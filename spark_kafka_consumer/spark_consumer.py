import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, unix_timestamp, avg, count
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)

# from utils import get_spark_session, get_logger, load_config
from config import config


def read_data_from_kafka(spark, config):
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
    )


def process_data(df):
    df_with_duration = df.withColumn(
        "view_duration",
        unix_timestamp(col("end_timestamp")) - unix_timestamp(col("start_timestamp")),
    )
    return (
        df_with_duration.withWatermark("end_timestamp", "1 minute")
        .groupBy(col("campaign_id"), col("end_timestamp").alias("minute_timestamp"))
        .agg(
            avg("view_duration").alias("avg_duration"),
            count("view_id").alias("total_count"),
        )
    )


def write_data_to_parquet(df, config):
    campaigns_df = df.spark.read.csv(
        config["spark"]["campaigns_csv_path"], header=True, inferSchema=True
    ).withColumnRenamed("id", "campaign_id")
    final_df = df.join(campaigns_df, on="campaign_id", how="left").select(
        col("campaign_id"),
        col("network_id"),
        col("minute_timestamp"),
        col("avg_duration"),
        col("total_count"),
    )

    final_df.writeStream.outputMode("update").format("parquet").option(
        "checkpointLocation", config["spark"]["checkpoint_location"]
    ).option("path", config["spark"]["reports_location"]).partitionBy(
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
    # config = load_config("config.yml")
    spark = get_spark_session()
    # logger = get_logger()

    df = read_data_from_kafka(spark, config)
    df.printSchema()
    df.writeStream.format("console").outputMode("append").option(
        "truncate", False
    ).trigger(processingTime="10 seconds").start().awaitTermination()
    # processed_df = process_data(df)
    # processed_df

    # write_data_to_parquet(processed_df, config)


if __name__ == "__main__":
    main()
