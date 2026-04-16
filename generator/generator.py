"""
generator.py - produces order and payment events to Kafka.

Uses plain JSON values so Spark can parse them directly with from_json().
Local jsonschema validation still enforces the contract before events are
published.
"""

import json
import os
import random
import time
import uuid

import jsonschema
from confluent_kafka import Producer


BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
PAYMENTS_TOPIC = os.getenv("PAYMENTS_TOPIC", "payments")
ORDERS_COUNT = int(os.getenv("ORDERS_COUNT", "0"))
INTERVAL_SEC = float(os.getenv("ORDERS_INTERVAL_SEC", "0.5"))

ORDER_SCHEMA_PATH = os.getenv("ORDER_SCHEMA_PATH", "schemas/order-value.schema.json")
PAYMENT_SCHEMA_PATH = os.getenv("PAYMENT_SCHEMA_PATH", "schemas/payment-value.schema.json")

USER_POOL = os.getenv("USER_POOL", "alice,bob,carol,dave,eve").split(",")
ITEM_PRICES = {
    "frozen yogurt": 5.99,
    "pizza": 12.99,
    "sushi": 18.99,
    "burger": 9.99,
    "ramen": 14.99,
}


def _load_schema(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


ORDER_SCHEMA = _load_schema(ORDER_SCHEMA_PATH)
PAYMENT_SCHEMA = _load_schema(PAYMENT_SCHEMA_PATH)


def build_order() -> dict:
    item = random.choice(list(ITEM_PRICES))
    return {
        "order_id": str(uuid.uuid4()),
        "user": random.choice(USER_POOL),
        "item": item,
        "quantity": random.randint(1, 5),
        "price": ITEM_PRICES[item],
    }


def build_payment(order_id: str, amount: float) -> dict:
    return {
        "payment_id": str(uuid.uuid4()),
        "order_id": order_id,
        "amount": round(amount, 2),
        "status": random.choices(
            ["success", "failed", "pending"],
            weights=[85, 10, 5],
        )[0],
    }


def validate(payload: dict, schema: dict) -> None:
    jsonschema.validate(instance=payload, schema=schema)


def delivery_report(err, msg) -> None:
    if err:
        print(f"[delivery-error] topic={msg.topic()} error={err}")
        return

    key = msg.key().decode() if msg.key() else "-"
    print(
        f"[delivered] topic={msg.topic():16s} key={key[:8]} "
        f"partition={msg.partition()} offset={msg.offset()}"
    )


def main() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    print(f"Generator starting: broker={BOOTSTRAP_SERVERS}")
    print(f"Topics: {ORDERS_TOPIC}, {PAYMENTS_TOPIC}")
    print(f"Users: {USER_POOL}")
    print(f"Count: {'infinite' if ORDERS_COUNT == 0 else ORDERS_COUNT}")
    print(f"Interval: {INTERVAL_SEC}s")

    sent = 0
    try:
        while ORDERS_COUNT == 0 or sent < ORDERS_COUNT:
            order = build_order()
            validate(order, ORDER_SCHEMA)

            producer.poll(0)
            producer.produce(
                topic=ORDERS_TOPIC,
                key=order["order_id"].encode(),
                value=json.dumps(order).encode(),
                callback=delivery_report,
            )

            if random.random() < 0.9:
                total = order["quantity"] * order["price"]
                payment = build_payment(order["order_id"], total)
                validate(payment, PAYMENT_SCHEMA)
                producer.produce(
                    topic=PAYMENTS_TOPIC,
                    key=payment["order_id"].encode(),
                    value=json.dumps(payment).encode(),
                    callback=delivery_report,
                )

            sent += 1
            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print("Generator stopped by user.")
    finally:
        undelivered = producer.flush()
        if undelivered:
            print(f"Warning: {undelivered} messages were not confirmed on shutdown.")


if __name__ == "__main__":
    main()
