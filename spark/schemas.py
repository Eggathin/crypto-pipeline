from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

TRADE_ENVELOPE = StructType([
    StructField("source", StringType()),
    StructField("ingested_at", DoubleType()),
    StructField("payload", StructType([
        StructField("type", StringType()),
        StructField("trade_id", LongType()),
        StructField("product_id", StringType()),
        StructField("side", StringType()),
        StructField("price", StringType()),
        StructField("size", StringType()),
        StructField("time", StringType()),
        StructField("sequence", LongType()),
        StructField("maker_order_id", StringType()),
        StructField("taker_order_id", StringType()),
    ])),
])