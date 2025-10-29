"""
Spark Structured Streaming Consumer (Public-safe)

- No hardcoded IPs or HDFS paths.
- Reads Kafka/HDFS config from environment variables or CLI args.
- Keeps your original schema and behavior, but configurable.

Env vars (defaults in brackets):
    KAFKA_BROKERS            [localhost:9092]
    KAFKA_TOPIC              [datalakex]
    STARTING_OFFSETS         [earliest]      # earliest | latest | {"topic":{"0":"-1"}}
    MAX_OFFSETS_PER_TRIGGER  [1000]
    HDFS_PATH                [/user/hadoop/raw_data/]
    HDFS_CHECKPOINT          [/user/hadoop/checkpoints_data/]
    SPARK_PACKAGES           [org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.commons:commons-pool2:2.11.1,org.slf4j:slf4j-api:1.7.36]

Usage:
    # Example (local HDFS / default configs)
    spark-submit spark/spark_hadoop_stream.py

    # Override via CLI:
    spark-submit spark/spark_hadoop_stream.py \
      --brokers localhost:9092 \
      --topic datalakex \
      --hdfs-path hdfs://localhost:9000/user/hadoop/raw_data \
      --checkpoint hdfs://localhost:9000/user/hadoop/checkpoints_data \
      --starting-offsets earliest \
      --max-offsets 1000

Requirements:
    pyspark==3.4.x (matching Scala 2.12)
"""

from __future__ import annotations
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType


def build_spark(app_name: str, packages: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", packages)
        .getOrCreate()
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Public-safe Spark Kafka→Parquet stream")
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "datalakex"))
    parser.add_argument("--starting-offsets", default=os.getenv("STARTING_OFFSETS", "earliest"))
    parser.add_argument("--max-offsets", type=int, default=int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "1000")))
    parser.add_argument("--hdfs-path", default=os.getenv("HDFS_PATH", "/user/hadoop/raw_data/"))
    parser.add_argument("--checkpoint", default=os.getenv("HDFS_CHECKPOINT", "/user/hadoop/checkpoints_data/"))
    parser.add_argument(
        "--spark-packages",
        default=os.getenv(
            "SPARK_PACKAGES",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "org.apache.commons:commons-pool2:2.11.1,"
            "org.slf4j:slf4j-api:1.7.36"
        )
    )
    return parser.parse_args()


def main():
    args = parse_args()

    spark = build_spark(app_name="data_lake", packages=args.spark_packages)

    # Incoming JSON schema 
    schema = (
        StructType()
        .add("name", StringType())
        .add("gender", StringType())
        .add("age", IntegerType())
        .add("country", StringType())
        .add("email", StringType())
    )

    # Read from Kafka
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.brokers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .load()
    )

    # Parse JSON value
    json_df = (
        df.selectExpr("CAST(value AS STRING) AS json_str")
          .select(from_json(col("json_str"), schema).alias("data"))
          .select("data.*")
    )

    # Write to Parquet 
    query = (
        json_df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("checkpointLocation", args.checkpoint)
        .option("path", args.hdfs_path)
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
