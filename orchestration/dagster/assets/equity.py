"""
Equity asset -- fetches missing OHLCV from Yahoo Finance and rebuilds Silver table.
Incremental per symbol: fetches dates after the latest date for each symbol in Silver.
Symbol list is data-driven from dim_instruments (is_current = TRUE, asset_class = equity).
New symbols added to dim_instruments are automatically picked up on next run.
"""
import duckdb
import json
import os
import time
import logging
import requests
from datetime import datetime, date, timezone
from dagster import asset, AssetExecutionContext, RetryPolicy, Backoff

log = logging.getLogger("dagster.equity")

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/transform/dbt/agora.duckdb")
BRONZE_FILE = os.getenv("BRONZE_FILE", "/app/polygon_bronze.jsonl")
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
DEFAULT_START = "2024-01-01"

def get_symbols_and_latest_dates() -> tuple[list[str], dict[str, str]]:
    """
    Read symbol list from dim_instruments (data-driven, automatic for new symbols).
    Get latest date per symbol from silver_equity_ohlcv_daily for incremental fetch.
    Falls back to DEFAULT_START for symbols with no existing Silver data.
    """
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        symbols = [
            r[0] for r in conn.execute("""
                SELECT DISTINCT symbol
                FROM agora.main_gold.dim_instruments
                WHERE is_current = TRUE
                  AND asset_class = 'equity'
                ORDER BY symbol
            """).fetchall()
        ]
        latest_dates = {
            r[0]: r[1].strftime("%Y-%m-%d")
            for r in conn.execute("""
                SELECT symbol, MAX(trade_date)::DATE as latest
                FROM agora.main.silver_equity_ohlcv_daily
                GROUP BY symbol
            """).fetchall()
        }
    finally:
        conn.close()
    return symbols, latest_dates

def fetch_yahoo_ohlcv(symbol: str, start_date: str) -> list[dict]:
    """Fetch daily OHLCV from Yahoo Finance from start_date to today."""
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
    end_ts = int(datetime.now().timestamp())
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"interval": "1d", "period1": start_ts, "period2": end_ts}
    for attempt in range(3):
        try:
            res = requests.get(url, params=params, headers=HEADERS, timeout=15)
            res.raise_for_status()
            data = res.json()
            result = data.get("chart", {}).get("result", [])
            if not result:
                return []
            r = result[0]
            timestamps = r.get("timestamp", [])
            quotes = r.get("indicators", {}).get("quote", [{}])[0]
            records = []
            for i, ts in enumerate(timestamps):
                dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                if dt <= start_date:
                    continue
                o = quotes.get("open", [None])[i]
                h = quotes.get("high", [None])[i]
                low_val = quotes.get("low", [None])[i]
                c = quotes.get("close", [None])[i]
                v = quotes.get("volume", [None])[i]
                if None in (o, h, low_val, c) or c <= 0:
                    continue
                records.append({
                    "symbol": symbol,
                    "date": dt,
                    "open": round(float(o), 4),
                    "high": round(float(h), 4),
                    "low": round(float(low_val), 4),
                    "close": round(float(c), 4),
                    "volume": int(v) if v else 0,
                    "vwap": None,
                    "trade_count": None,
                    "timestamp_ms": ts * 1000,
                    "ingested_at_ms": int(datetime.now().timestamp() * 1000),
                    "source": "yahoo",
                    "adjusted": True,
                })
            return records
        except Exception as e:
            log.warning(f"Yahoo attempt {attempt+1} for {symbol}: {e}")
            time.sleep(0.5)
    return []

@asset(
    group_name="equity",
    description="Fetch missing equity OHLCV from Yahoo Finance and rebuild Silver table. Symbol list driven from dim_instruments.",
    retry_policy=RetryPolicy(max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL),
)
def silver_equity_ohlcv_daily(context: AssetExecutionContext):
    """Incremental per-symbol fetch from Yahoo Finance, append to Bronze, rebuild Silver."""
    symbols, latest_dates = get_symbols_and_latest_dates()
    today = date.today().strftime("%Y-%m-%d")
    context.log.info(f"Found {len(symbols)} symbols from dim_instruments.")

    new_records = 0
    skipped = 0
    failed = 0

    with open(BRONZE_FILE, "a") as f:
        for i, symbol in enumerate(symbols):
            start_date = latest_dates.get(symbol, DEFAULT_START)
            if start_date >= today:
                skipped += 1
                continue
            records = fetch_yahoo_ohlcv(symbol, start_date)
            if records:
                for rec in records:
                    f.write(json.dumps(rec) + "\n")
                new_records += len(records)
                context.log.info(f"[{i+1}/{len(symbols)}] {symbol}: +{len(records)} rows (from {start_date})")
            else:
                failed += 1
                context.log.warning(f"[{i+1}/{len(symbols)}] {symbol}: no data returned (delisted or rate limited)")
            time.sleep(0.3)

    context.log.info(f"Fetch complete. New: {new_records} rows, Skipped: {skipped}, Failed: {failed}")
    context.log.info("Rebuilding Silver table...")

    conn = duckdb.connect(DUCKDB_PATH)
    try:
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
        sym_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM agora.main.silver_equity_ohlcv_daily").fetchone()[0]
    finally:
        conn.close()

    context.log.info(f"Silver rebuilt: {count} rows, {sym_count} symbols")
    return {"new_records": new_records, "total_rows": count, "symbols": sym_count, "skipped": skipped, "failed": failed}