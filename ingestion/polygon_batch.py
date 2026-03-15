"""
Polygon.io Batch Ingester
--------------------------
Pulls daily OHLCV history for S&P 500 tickers from Polygon.io free tier
Writes raw data to Bronze Iceberg table: bronze.equity_ohlcv_daily

Free tier limits: 5 API calls/minute
We handle this with automatic rate limiting and retries.
"""

import json
import logging
import os
import time
from datetime import date, datetime, timezone
from typing import Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("polygon.ingester")

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
POLYGON_BASE    = "https://api.polygon.io"

# Free tier: 5 calls/min — we wait 13s between calls to be safe
RATE_LIMIT_WAIT = 13

# 2 years of history
FROM_DATE = "2024-01-01"
TO_DATE   = date.today().isoformat()

# Top 50 S&P 500 tickers
SP500_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META",
    "GOOGL", "GOOG", "BRK.B", "LLY", "JPM",
    "V", "XOM", "UNH", "MA", "AVGO",
    "PG", "JNJ", "HD", "MRK", "COST",
    "ABBV", "CVX", "CRM", "BAC", "NFLX",
    "WMT", "AMD", "PEP", "KO", "TMO",
    "ACN", "MCD", "CSCO", "ABT", "TXN",
    "ADBE", "DHR", "WFC", "LIN", "NKE",
    "NEE", "PM", "ORCL", "RTX", "QCOM",
    "INTC", "INTU", "AMGN", "IBM", "GE",
]

OUTPUT_FILE = "polygon_bronze.jsonl"


def fetch_daily_ohlcv(ticker: str, from_date: str, to_date: str) -> list:
    """Fetch daily OHLCV bars for a ticker from Polygon.io."""
    url = f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": POLYGON_API_KEY,
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "ERROR":
            log.error(f"{ticker}: API error — {data.get('error')}")
            return []

        results = data.get("results", [])
        log.info(f"{ticker}: fetched {len(results)} days of OHLCV data")
        return results

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            log.warning(f"{ticker}: rate limited — waiting 60s")
            time.sleep(60)
            return fetch_daily_ohlcv(ticker, from_date, to_date)
        log.error(f"{ticker}: HTTP error — {e}")
        return []
    except Exception as e:
        log.error(f"{ticker}: unexpected error — {e}")
        return []


def transform_bar(ticker: str, bar: dict) -> dict:
    """Transform Polygon bar to our Bronze schema."""
    return {
        "symbol":         ticker,
        "date":           datetime.fromtimestamp(bar["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
        "open":           bar["o"],
        "high":           bar["h"],
        "low":            bar["l"],
        "close":          bar["c"],
        "volume":         bar["v"],
        "vwap":           bar.get("vw"),
        "trade_count":    bar.get("n"),
        "timestamp_ms":   bar["t"],
        "ingested_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
        "source":         "polygon",
        "adjusted":       True,
    }


def run():
    if not POLYGON_API_KEY:
        log.error("POLYGON_API_KEY not set — check your .env file")
        return

    log.info(f"Starting Polygon batch ingestion")
    log.info(f"Tickers: {len(SP500_TICKERS)} | Range: {FROM_DATE} to {TO_DATE}")
    log.info(f"Output: {OUTPUT_FILE}")

    total_records = 0
    failed_tickers = []

    with open(OUTPUT_FILE, "w") as f:
        for i, ticker in enumerate(SP500_TICKERS):
            log.info(f"[{i+1}/{len(SP500_TICKERS)}] Fetching {ticker}...")

            bars = fetch_daily_ohlcv(ticker, FROM_DATE, TO_DATE)

            if not bars:
                failed_tickers.append(ticker)
            else:
                for bar in bars:
                    record = transform_bar(ticker, bar)
                    f.write(json.dumps(record) + "\n")
                    total_records += 1

            # Rate limit: 5 calls/min on free tier
            if i < len(SP500_TICKERS) - 1:
                log.info(f"Waiting {RATE_LIMIT_WAIT}s (rate limit)...")
                time.sleep(RATE_LIMIT_WAIT)

    log.info(f"Done. Total records: {total_records}")
    if failed_tickers:
        log.warning(f"Failed tickers: {failed_tickers}")

    # Print sample
    log.info("Sample output:")
    with open(OUTPUT_FILE) as f:
        for i, line in enumerate(f):
            if i >= 3:
                break
            record = json.loads(line)
            log.info(f"  {record['symbol']} {record['date']} O={record['open']} H={record['high']} L={record['low']} C={record['close']} V={record['volume']}")


if __name__ == "__main__":
    run()

