import os

from pyspark.sql import SparkSession

from dotenv import load_dotenv

load_dotenv()

SPARK_VERSION = "3.5.3"

PACKAGES = [
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
    "org.apache.hadoop:hadoop-aws:3.3.4",
]


def get_spark(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.jars.packages", ",".join(PACKAGES))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://localhost:8333"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )