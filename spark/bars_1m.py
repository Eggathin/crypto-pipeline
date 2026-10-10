import os

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from schemas import TRADE_ENVELOPE
from spark_session import get_spark

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = os.environ.get("TOPIC", "crypto.trades.raw")
LAKE = os.environ.get("LAKE_ROOT", "s3a://lake")

AMOUNT = DecimalType(20, 10)


def read_trades(spark):
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

    return (
        raw
        .select(F.from_json(F.col("value").cast("string"), TRADE_ENVELOPE).alias("e"))
        .select("e.payload.*")
        .where(F.col("type") == "match")
        .select(
            "product_id",
            "trade_id",
            F.to_timestamp("time").alias("event_time"),
            F.col("price").cast(AMOUNT).alias("price"),
            F.col("size").cast(AMOUNT).alias("size"),
        )
        .where(F.col("event_time").isNotNull())
    )


def build_bars(trades):
    return (
        trades
        .withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "1 minute"), "product_id")
        .agg(
            F.min(F.struct("event_time", "trade_id", "price")).alias("first_trade"),
            F.max(F.struct("event_time", "trade_id", "price")).alias("last_trade"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.sum("size").alias("volume"),
            F.sum(F.col("price") * F.col("size")).alias("notional"),
            F.count("*").alias("trade_count"),
        )
        .select(
            F.col("window.start").alias("bar_start"),
            F.col("window.end").alias("bar_end"),
            "product_id",
            F.col("first_trade.price").alias("open"),
            "high",
            "low",
            F.col("last_trade.price").alias("close"),
            "volume",
            (F.col("notional") / F.col("volume")).alias("vwap"),
            "trade_count",
            F.to_date("window.start").alias("bar_date"),
        )
    )


def main():
    spark = get_spark("bars-1m")
    bars = build_bars(read_trades(spark))

    query = (
        bars.writeStream
        .format("parquet")
        .option("path", f"{LAKE}/silver/bars_1m")
        .option("checkpointLocation", f"{LAKE}/_checkpoints/bars_1m")
        .partitionBy("bar_date", "product_id")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()