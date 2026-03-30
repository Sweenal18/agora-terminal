"""
Agora Terminal -- CDC Asset
Consumes Debezium change events from Kafka topic agora.public.instruments
and upserts into DuckDB silver layer instruments table.
"""
import json
import os
from datetime import datetime, timezone

import duckdb
from dagster import asset, AssetExecutionContext, Output, MetadataValue
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = "agora.public.instruments"
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/data/agora.duckdb")
CONSUMER_GROUP = "agora-cdc-dagster"


def _parse_ts(raw_ts) -> str:
    """Convert Debezium microsecond timestamp to ISO string."""
    if raw_ts is None:
        return datetime.now(timezone.utc).isoformat()
    try:
        return datetime.fromtimestamp(int(raw_ts) / 1_000_000, tz=timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS agora.main;
        CREATE TABLE IF NOT EXISTS agora.main.silver_instruments (
            symbol VARCHAR PRIMARY KEY,
            company_name VARCHAR,
            sector VARCHAR,
            industry VARCHAR,
            exchange VARCHAR,
            country VARCHAR,
            currency VARCHAR,
            asset_class VARCHAR,
            market_cap DOUBLE,
            cdc_updated_at TIMESTAMP,
            cdc_deleted BOOLEAN DEFAULT FALSE,
            ingested_at TIMESTAMP DEFAULT NOW()
        )
    """)


def _upsert_record(conn: duckdb.DuckDBPyConnection, record: dict) -> str:
    """Upsert a single CDC record into DuckDB."""
    is_deleted = record.get("__deleted", "false") == "true"
    symbol = record.get("symbol")
    if not symbol:
        return "skip"

    updated_at = _parse_ts(record.get("updated_at"))

    conn.execute("""
        INSERT INTO agora.main.silver_instruments
            (symbol, company_name, sector, industry, exchange, country,
             currency, asset_class, market_cap, cdc_updated_at, cdc_deleted, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
        ON CONFLICT (symbol) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            sector = EXCLUDED.sector,
            industry = EXCLUDED.industry,
            exchange = EXCLUDED.exchange,
            country = EXCLUDED.country,
            currency = EXCLUDED.currency,
            asset_class = EXCLUDED.asset_class,
            market_cap = EXCLUDED.market_cap,
            cdc_updated_at = EXCLUDED.cdc_updated_at,
            cdc_deleted = EXCLUDED.cdc_deleted,
            ingested_at = NOW()
    """, [
        symbol,
        record.get("company_name"),
        record.get("sector"),
        record.get("industry"),
        record.get("exchange"),
        record.get("country"),
        record.get("currency"),
        record.get("asset_class"),
        record.get("market_cap"),
        updated_at,
        is_deleted,
    ])
    return "delete" if is_deleted else "upsert"


@asset(
    group_name="cdc",
    description="Consume Debezium CDC events from Kafka and upsert into DuckDB silver_instruments",
)
def silver_instruments_cdc(context: AssetExecutionContext) -> Output[dict]:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=15000,
    )

    conn = duckdb.connect(DUCKDB_PATH)
    _ensure_table(conn)

    counts = {"upsert": 0, "delete": 0, "skip": 0}
    try:
        for msg in consumer:
            record = msg.value
            if record is None:
                continue
            result = _upsert_record(conn, record)
            counts[result] += 1
            context.log.info(f"[{result.upper()}] symbol={record.get('symbol')} sector={record.get('sector')}")
    finally:
        consumer.close()
        conn.close()

    total = counts["upsert"] + counts["delete"]
    context.log.info(f"CDC run complete: {counts['upsert']} upserts, {counts['delete']} deletes, {counts['skip']} skipped")

    conn_ro = duckdb.connect(DUCKDB_PATH, read_only=True)
    row_count = conn_ro.execute("SELECT COUNT(*) FROM agora.main.silver_instruments").fetchone()[0]
    conn_ro.close()

    return Output(
        value=counts,
        metadata={
            "events_processed": MetadataValue.int(total),
            "upserts": MetadataValue.int(counts["upsert"]),
            "deletes": MetadataValue.int(counts["delete"]),
            "table_row_count": MetadataValue.int(row_count),
            "kafka_topic": MetadataValue.text(KAFKA_TOPIC),
        },
    )