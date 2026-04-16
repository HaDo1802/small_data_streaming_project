"""
aggregator.py - aggregates order events into tumbling window metrics.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    from_json,
    round as spark_round,
    sum as spark_sum,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/streaming")
POSTGRES_USER = os.getenv("POSTGRES_USER", "streamer")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "streamer")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/opt/project/checkpoints/aggregator")

WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "30 seconds")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")


ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("user", StringType()),
        StructField("item", StringType()),
        StructField("quantity", IntegerType()),
        StructField("price", DoubleType()),
    ]
)


spark = SparkSession.builder.appName("OrderAggregator").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", ORDERS_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

orders = (
    raw.select(
        from_json(col("value").cast("string"), ORDER_SCHEMA).alias("payload"),
        col("timestamp").alias("event_time"),
    )
    .select("payload.*", "event_time")
    .filter(col("order_id").isNotNull())
)

windowed = (
    orders.withWatermark("event_time", WATERMARK_DELAY)
    .groupBy(window(col("event_time"), WINDOW_DURATION), col("user"))
    .agg(
        count("*").alias("order_count"),
        spark_round(spark_sum(col("quantity") * col("price")), 2).alias("revenue"),
    )
)

console_query = (
    windowed.writeStream.queryName("console_out")
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)


def write_to_postgres(batch_df, batch_id: int) -> None:
    rows = batch_df.count()
    if rows == 0:
        return

    (
        batch_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("user"),
            col("order_count"),
            col("revenue"),
        )
        .write.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "order_metrics")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    print(f"[batch {batch_id}] wrote {rows} rows to postgres")


postgres_query = (
    windowed.writeStream.queryName("postgres_out")
    .outputMode("update")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

print(f"Aggregator running; waiting for events on topic: {ORDERS_TOPIC}")
spark.streams.awaitAnyTermination()
