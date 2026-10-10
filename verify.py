import duckdb

con = duckdb.connect()
con.sql("SET TimeZone = 'UTC'")
con.sql("INSTALL httpfs")
con.sql("LOAD httpfs")
con.sql("""
    CREATE SECRET lake (
        TYPE s3,
        KEY_ID 'localdev',
        SECRET 'localdevsecret',
        ENDPOINT 'localhost:8333',
        URL_STYLE 'path',
        USE_SSL false
    )
""")

con.sql("""
    CREATE VIEW bronze AS
    SELECT * FROM read_parquet('s3://lake/bronze/trades/**/*.parquet', hive_partitioning = true)
""")
con.sql("""
    CREATE VIEW bars AS
    SELECT * FROM read_parquet('s3://lake/silver/bars_1m/**/*.parquet', hive_partitioning = true)
""")

print(con.sql("""
    SELECT bar_start, product_id, open, high, low, close, volume, vwap, trade_count
    FROM bars
    ORDER BY bar_start DESC, product_id
    LIMIT 10
"""))

print(con.sql("""
    WITH raw AS (
        SELECT
            product_id,
            date_trunc('minute', CAST(json_extract_string(value, '$.payload.time') AS TIMESTAMPTZ)) AS minute,
            CAST(json_extract_string(value, '$.payload.size') AS DECIMAL(20, 10)) AS size
        FROM bronze
        WHERE json_extract_string(value, '$.payload.type') = 'match'
    ),
    raw_agg AS (
        SELECT product_id, minute, SUM(size) AS volume, COUNT(*) AS trade_count
        FROM raw
        GROUP BY ALL
    )
    SELECT
        r.product_id,
        r.minute,
        r.trade_count AS bronze_trades,
        b.trade_count AS bar_trades,
        r.volume - b.volume AS volume_diff
    FROM raw_agg r
    JOIN bars b ON b.product_id = r.product_id AND b.bar_start = r.minute
    WHERE r.trade_count <> b.trade_count OR r.volume <> b.volume
    ORDER BY r.minute DESC
"""))