"""
Bronze Iceberg Writer
---------------------
Reads raw ticks from Kafka topic: crypto.ticks.raw
Writes them to an Iceberg table in MinIO: bronze.crypto_ticks
"""

import json
import logging
import os
import signal

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("bronze.writer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC_CRYPTO_TICKS", "crypto.ticks.raw")
KAFKA_GROUP     = "bronze-iceberg-writer"

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ROOT_USER", "agora")
MINIO_SECRET    = os.getenv("MINIO_ROOT_PASSWORD", "agora_secret")
MINIO_BUCKET    = "agora-bronze"

BATCH_SIZE      = int(os.getenv("BRONZE_BATCH_SIZE", "500"))

BRONZE_SCHEMA = Schema(
    NestedField(1, "symbol",         StringType(), required=True),
    NestedField(2, "price",          DoubleType(), required=True),
    NestedField(3, "quantity",       DoubleType(), required=True),
    NestedField(4, "trade_id",       LongType(),   required=True),
    NestedField(5, "timestamp_ms",   LongType(),   required=True),
    NestedField(6, "ingested_at_ms", LongType(),   required=True),
    NestedField(7, "side",           StringType(), required=True),
    NestedField(8, "source",         StringType(), required=True),
)

ARROW_SCHEMA = pa.schema([
    pa.field("symbol",         pa.string(),  nullable=False),
    pa.field("price",          pa.float64(), nullable=False),
    pa.field("quantity",       pa.float64(), nullable=False),
    pa.field("trade_id",       pa.int64(),   nullable=False),
    pa.field("timestamp_ms",   pa.int64(),   nullable=False),
    pa.field("ingested_at_ms", pa.int64(),   nullable=False),
    pa.field("side",           pa.string(),  nullable=False),
    pa.field("source",         pa.string(),  nullable=False),
])


def get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "bronze",
        **{
            "uri": "sqlite:////tmp/iceberg_catalog.db",
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS,
            "s3.secret-access-key": MINIO_SECRET,
            "s3.path-style-access": "true",
            "warehouse": f"s3://{MINIO_BUCKET}/warehouse",
        },
    )


def ensure_table(catalog: SqlCatalog):
    if ("bronze",) not in catalog.list_namespaces():
        catalog.create_namespace("bronze")
        log.info("Created namespace: bronze")

    try:
        catalog.load_table("bronze.crypto_ticks")
        log.info("Iceberg table already exists: bronze.crypto_ticks")
    except NoSuchTableError:
        catalog.create_table(
            "bronze.crypto_ticks",
            schema=BRONZE_SCHEMA,
            location=f"s3://{MINIO_BUCKET}/warehouse/bronze/crypto_ticks",
        )
        log.info("Created Iceberg table: bronze.crypto_ticks")


def write_batch(table, records: list) -> None:
    df = pa.table(
        {
            "symbol":         [r["symbol"] for r in records],
            "price":          [r["price"] for r in records],
            "quantity":       [r["quantity"] for r in records],
            "trade_id":       [r["trade_id"] for r in records],
            "timestamp_ms":   [r["timestamp_ms"] for r in records],
            "ingested_at_ms": [r["ingested_at_ms"] for r in records],
            "side":           [r["side"] for r in records],
            "source":         ["binance" for _ in records],
        },
        schema=ARROW_SCHEMA,
    )
    table.append(df)
    log.info(f"Wrote {len(records)} records to bronze.crypto_ticks")


def run():
    log.info("Starting Bronze Iceberg Writer")
    log.info(f"Kafka: {KAFKA_BOOTSTRAP} | Topic: {KAFKA_TOPIC}")
    log.info(f"MinIO: {MINIO_ENDPOINT} | Bucket: {MINIO_BUCKET}")

    catalog = get_catalog()
    ensure_table(catalog)
    iceberg_table = catalog.load_table("bronze.crypto_ticks")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])

    running = True
    buffer = []

    def shutdown(sig, frame):
        nonlocal running
        log.info("Shutdown signal — flushing buffer...")
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    total = 0
    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error(f"Kafka error: {msg.error()}")
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
                buffer.append(record)
            except Exception as e:
                log.error(f"Failed to parse message: {e}")
                continue

            if len(buffer) >= BATCH_SIZE:
                write_batch(iceberg_table, buffer)
                consumer.commit()
                total += len(buffer)
                log.info(f"Total records written: {total}")
                buffer.clear()

    finally:
        if buffer:
            write_batch(iceberg_table, buffer)
            consumer.commit()
            total += len(buffer)
            log.info(f"Flushed final batch. Total written: {total}")
        consumer.close()
        log.info("Bronze writer stopped.")


if __name__ == "__main__":
    run()
