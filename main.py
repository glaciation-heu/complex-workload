import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


def run_complex_workload(
    run_time: int,
    minio_host: str,
    minio_access_key: str,
    minio_secret_key: str,
    source_bucket: str,
    target_bucket: str,
    dataset_name: str,
) -> None:
    print("Starting")

    spark = SparkSession.builder.appName("Multiplication Program").getOrCreate()

    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key", minio_access_key
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key", minio_secret_key
    )
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_host)
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.path.style.access", "true"
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.connection.ssl.enabled", "false"
    )

    print("Running")

    print("run_time: {run_time}")
    print("minio_host: {minio_host}")
    print("minio_access_key: ***")
    print("minio_secret_key: ***")
    print("source_bucket: {source_bucket}")
    print("target_bucket: {target_bucket}")
    print("dataset_name: {dataset_name}")

    timeout = time.time() + run_time
    index = 1

    # To ensure the workload runs for a specified duration, the read operation
    # from the source bucket, calculation, and copying to the target bucket are
    # performed in a loop.
    while True:
        print(".", end="", flush=True)

        if time.time() > timeout:
            break

        # Read the dataset from the source bucket
        df = spark.read.csv(f"s3a://{source_bucket}/{dataset_name}")

        # Calculate the sum of all numbers in each column
        sums = df.select(
            *[
                sum(col(col_name)).alias(col_name)
                for col_name in df.columns
                if df.schema[col_name].dataType != "string"
            ]
        )

        # Write the result to the target bucket
        sums.write.csv(
            f"s3a://{target_bucket}/{dataset_name}_sums_{index}",
            mode="overwrite",
        )

        index += 1

    spark.stop()
    print("\nSuccessfully completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Complex workload.")

    parser.add_argument("time", type=int, help="Duration of work, sec.")
    parser.add_argument("minio_host", type=str, help="Minio host address")
    parser.add_argument("minio_access_key", type=str, help="Minio access key")
    parser.add_argument("minio_secret_key", type=str, help="Minio secret key")
    parser.add_argument("source_bucket", type=str, help="Name of a source bucket")
    parser.add_argument("target_bucket", type=str, help="Name of a target bucket")
    parser.add_argument("dataset_name", type=str, help="Name of a dataset file")

    args = parser.parse_args()

    run_complex_workload(
        args.time,
        args.minio_host,
        args.minio_access_key,
        args.minio_secret_key,
        args.source_bucket,
        args.target_bucket,
        args.dataset_name,
    )
