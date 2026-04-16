"""
stream_enricher.py - joins orders and payments and writes orders-enriched.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, struct, to_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BROKER = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
PAYMENTS_TOPIC = os.getenv("PAYMENTS_TOPIC", "payments")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "orders-enriched")
CHECKPOINT_DIR = os.getenv(
    "CHECKPOINT_DIR",
    "/opt/project/checkpoints/stream_enricher",
)


ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("user", StringType()),
        StructField("item", StringType()),
        StructField("quantity", IntegerType()),
        StructField("price", DoubleType()),
    ]
)

PAYMENT_SCHEMA = StructType(
    [
        StructField("payment_id", StringType()),
        StructField("order_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("status", StringType()),
    ]
)


spark = SparkSession.builder.appName("StreamEnricher").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def read_topic(topic: str, schema: StructType):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            from_json(col("value").cast("string"), schema).alias("payload"),
            col("timestamp"),
        )
        .select("payload.*", col("timestamp"))
        .filter(col(schema.fields[0].name).isNotNull())
    )


orders = read_topic(ORDERS_TOPIC, ORDER_SCHEMA)
payments = read_topic(PAYMENTS_TOPIC, PAYMENT_SCHEMA)

orders_wm = orders.withWatermark("timestamp", "2 minutes").alias("o")
payments_wm = payments.withWatermark("timestamp", "2 minutes").alias("p")

enriched = orders_wm.join(
    payments_wm,
    expr(
        """
        o.order_id = p.order_id
        AND p.timestamp BETWEEN o.timestamp AND o.timestamp + INTERVAL 3 MINUTES
        """
    ),
    how="inner",
).select(
    col("o.order_id"),
    col("o.user"),
    col("o.item"),
    col("o.quantity"),
    col("o.price"),
    col("p.payment_id"),
    col("p.amount"),
    col("p.status").alias("payment_status"),
    col("o.timestamp").alias("order_time"),
)

kafka_ready = enriched.select(
    col("order_id").alias("key"),
    to_json(
        struct(
            col("order_id"),
            col("user"),
            col("item"),
            col("quantity"),
            col("price"),
            col("payment_id"),
            col("amount"),
            col("payment_status"),
            col("order_time"),
        )
    ).alias("value"),
)

(
    kafka_ready.writeStream.queryName("enricher_out")
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="10 seconds")
    .start()
    .awaitTermination()
)
