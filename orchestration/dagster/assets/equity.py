"""
Equity asset — refreshes Silver OHLCV table from polygon_bronze.jsonl
"""
import duckdb
import os
from dagster import asset, AssetExecutionContext

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/transform/dbt/agora.duckdb")
BRONZE_FILE = os.getenv("BRONZE_FILE", "/app/polygon_bronze.jsonl")

@asset(
    group_name="equity",
    description="Rebuild silver_equity_ohlcv_daily from polygon_bronze.jsonl",
)
def silver_equity_ohlcv_daily(context: AssetExecutionContext):
    """Rebuild Silver OHLCV table from Bronze jsonl file."""
    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute(f"""
        CREATE OR REPLACE TABLE agora.main.silver_equity_ohlcv_daily AS
        SELECT
            symbol,
            date::DATE           AS trade_date,
            open::DOUBLE         AS open,
            high::DOUBLE         AS high,
            low::DOUBLE          AS low,
            close::DOUBLE        AS close,
            volume::BIGINT       AS volume,
            vwap::DOUBLE         AS vwap,
            trade_count::INTEGER AS trade_count,
            source::VARCHAR      AS source,
            adjusted::BOOLEAN    AS adjusted,
            CURRENT_TIMESTAMP    AS processed_at
        FROM read_json_auto('{BRONZE_FILE}')
        WHERE close > 0 AND volume > 0 AND open > 0 AND high >= low AND symbol IS NOT NULL
    """)
    count = conn.execute("SELECT COUNT(*) FROM agora.main.silver_equity_ohlcv_daily").fetchone()[0]
    symbols = conn.execute("SELECT COUNT(DISTINCT symbol) FROM agora.main.silver_equity_ohlcv_daily").fetchone()[0]
    conn.close()
    context.log.info(f"Rebuilt silver_equity_ohlcv_daily: {count} rows, {symbols} symbols")
    return {"rows": count, "symbols": symbols}