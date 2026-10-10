import os

from pyspark.sql import functions as F

from spark_session import get_spark

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = os.environ.get("TOPIC", "crypto.trades.raw")
LAKE = os.environ.get("LAKE_ROOT", "s3a://lake")


def main():
    spark = get_spark("bronze-trades")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50000)
        .option("failOnDataLoss", "true")
        .load()
    )

    bronze = raw.select(
        F.col("key").cast("string").alias("product_id"),
        F.col("value").cast("string").alias("value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.to_date("timestamp").alias("ingest_date"),
    )

    query = (
        bronze.writeStream
        .format("parquet")
        .option("path", f"{LAKE}/bronze/trades")
        .option("checkpointLocation", f"{LAKE}/_checkpoints/bronze_trades")
        .partitionBy("ingest_date", "product_id")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()