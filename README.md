# Streaming Project

Real-time analytics demo built around a 3-broker Kafka cluster, Spark Structured Streaming, and Postgres.

## Run

Start the base demo:

```bash
docker compose up -d --build
```

Watch the generator and aggregator:

```bash
docker compose logs -f generator
docker compose logs -f aggregator
```

Inspect the Kafka topics:

```bash
docker exec -it kafka_1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --describe
```

Query the Postgres output:

```bash
docker exec -it postgres psql -U streamer -d streaming -c \
  "SELECT * FROM latest_metrics ORDER BY window_start DESC;"
```

Connect from your host machine:

```bash
psql -h localhost -p 5433 -U streamer -d streaming
```

Start the join job:

```bash
docker compose --profile enrich up -d stream-enricher
```

Read the enriched stream:

```bash
docker exec -it kafka_1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --topic orders-enriched \
  --from-beginning
```

Reset everything:

```bash
docker compose down -v
rm -rf checkpoints
```

## Short Experiments

Run the generator faster:

```bash
docker compose run --rm -e ORDERS_INTERVAL_SEC=0.1 generator
```

Bias traffic toward one user:

```bash
docker compose run --rm -e USER_POOL=alice,alice,alice,bob generator
```

Replay the aggregator from scratch:

```bash
rm -rf checkpoints/aggregator
docker compose restart aggregator
```

## Where To Look

- Kafka replication and ISR: Kafka UI at `http://localhost:8080`
- Spark execution, stages, and backpressure: Spark UI at `http://localhost:4040`
- Spark cluster layout: Spark Master UI at `http://localhost:8081`
- Aggregated metrics sink: [postgres/init.sql](/Users/hado/Desktop/Career/Coding/Data Engineer/Project/local_data_streaming/postgres/init.sql:1)
- Streaming logic: [spark/aggregator.py](/Users/hado/Desktop/Career/Coding/Data Engineer/Project/local_data_streaming/spark/aggregator.py:1) and [spark/stream_enricher.py](/Users/hado/Desktop/Career/Coding/Data Engineer/Project/local_data_streaming/spark/stream_enricher.py:1)
