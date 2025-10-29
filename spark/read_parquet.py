"""
Read Parquet from HDFS (Public-safe)

- No hardcoded endpoints: takes HDFS/defaultFS and path from env or CLI.
- Shows data, schema, and count.

Env vars (defaults in brackets):
  HDFS_DEFAULT_FS   [hdfs://localhost:9000]
  PARQUET_PATH      [/user/hadoop/raw_data]

Usage:
  # defaults
  spark-submit spark/read_parquet.py

  # override via CLI
  spark-submit spark/read_parquet.py \
    --default-fs hdfs://localhost:9000 \
    --path /user/hadoop/raw_data
"""

from __future__ import annotations
import os
import argparse
from pyspark.sql import SparkSession


def parse_args():
    p = argparse.ArgumentParser(description="Read Parquet from HDFS (public-safe)")
    p.add_argument("--default-fs",
                   default=os.getenv("HDFS_DEFAULT_FS", "hdfs://localhost:9000"),
                   help="Hadoop defaultFS (e.g., hdfs://localhost:9000)")
    p.add_argument("--path",
                   default=os.getenv("PARQUET_PATH", "/user/hadoop/raw_data"),
                   help="Parquet directory path (relative to defaultFS if not absolute)")
    return p.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("ReadParquetFromHDFS")
        .config("spark.hadoop.fs.defaultFS", args.default_fs)
        .getOrCreate()
    )

    # Build full path if needed
    parquet_path = args.path
    if parquet_path.startswith("/"):
        parquet_path = f"{args.default_fs.rstrip('/')}{parquet_path}"

    df = spark.read.parquet(parquet_path)

    df.show(truncate=False)
    df.printSchema()
    print(df.count())

    spark.stop()


if __name__ == "__main__":
    main()
