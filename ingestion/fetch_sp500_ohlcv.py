"""
Agora Terminal - Full S&P 500 OHLCV Expander
Fetches 2 years of daily OHLCV for all 503 S&P 500 stocks from Polygon.io
Writes directly into agora.main.silver_equity_ohlcv_daily in DuckDB
Skips tickers already present - safe to resume if interrupted

Runtime: ~110 minutes (free tier rate limit)
Usage: python ingestion/fetch_sp500_ohlcv.py
"""

import logging
import time
import requests
import duckdb
from datetime import date, datetime, timezone
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sp500_expander")

POLYGON_API_KEY = "hfYRQmwQvEhNkMFQj19TNPwfFS5qhdte"
POLYGON_BASE    = "https://api.polygon.io"
DUCKDB_PATH     = "transform/dbt/agora.duckdb"
FROM_DATE       = "2024-01-01"
TO_DATE         = date.today().isoformat()
RATE_LIMIT_WAIT = 13  # 5 calls/min free tier


def get_sp500_tickers() -> list[str]:
    log.info("Fetching S&P 500 ticker list from Wikipedia...")
    resp = requests.get(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        headers={"User-Agent": "Mozilla/5.0"}, timeout=15
    )
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"id": "constituents"})
    tickers = []
    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if cols:
            ticker = cols[0].text.strip().replace(".", "-")
            tickers.append(ticker)
    log.info(f"Found {len(tickers)} S&P 500 tickers")
    return tickers


def get_existing_tickers(conn) -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT symbol FROM agora.main.silver_equity_ohlcv_daily"
    ).fetchall()
    return {r[0] for r in rows}


def fetch_ohlcv(ticker: str) -> list[dict]:
    url = f"{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{FROM_DATE}/{TO_DATE}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": POLYGON_API_KEY}
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") == "ERROR":
            log.error(f"{ticker}: API error - {data.get('error')}")
            return []
        results = data.get("results", [])
        log.info(f"  {ticker}: {len(results)} bars")
        return results
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            log.warning(f"{ticker}: rate limited - waiting 60s")
            time.sleep(60)
            return fetch_ohlcv(ticker)
        log.error(f"{ticker}: HTTP {e.response.status_code}")
        return []
    except Exception as e:
        log.error(f"{ticker}: {e}")
        return []


def insert_bars(conn, ticker: str, bars: list[dict]):
    rows = []
    for bar in bars:
        trade_date = datetime.fromtimestamp(
            bar["t"] / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d")
        rows.append((
            ticker,
            trade_date,
            bar["o"], bar["h"], bar["l"], bar["c"],
            int(bar["v"]),
            bar.get("vw"),
            bar.get("n"),
            "polygon",
            True,
            datetime.now(timezone.utc),
        ))

    conn.executemany("""
        INSERT INTO agora.main.silver_equity_ohlcv_daily
        (symbol, trade_date, open, high, low, close, volume, vwap,
         trade_count, source, adjusted, processed_at)
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM agora.main.silver_equity_ohlcv_daily
            WHERE symbol = ? AND trade_date = ?
        )
    """, [(r + (r[0], r[1])) for r in rows])


def main():
    tickers = get_sp500_tickers()
    conn = duckdb.connect(DUCKDB_PATH)

    existing = get_existing_tickers(conn)
    todo = [t for t in tickers if t not in existing]
    skipped = len(tickers) - len(todo)

    log.info(f"Total: {len(tickers)} | Already loaded: {skipped} | To fetch: {len(todo)}")
    log.info(f"Estimated time: {len(todo) * RATE_LIMIT_WAIT / 60:.0f} minutes")

    success, failed = 0, []

    for i, ticker in enumerate(todo):
        log.info(f"[{i+1}/{len(todo)}] Fetching {ticker}...")
        bars = fetch_ohlcv(ticker)

        if not bars:
            failed.append(ticker)
        else:
            insert_bars(conn, ticker, bars)
            success += 1

        if i < len(todo) - 1:
            time.sleep(RATE_LIMIT_WAIT)

    conn.close()

    log.info(f"Done. Success: {success} | Failed: {len(failed)}")
    if failed:
        log.warning(f"Failed tickers: {failed}")

    # Show final count
    conn2 = duckdb.connect(DUCKDB_PATH, read_only=True)
    total = conn2.execute("SELECT COUNT(DISTINCT symbol) FROM agora.main.silver_equity_ohlcv_daily").fetchone()[0]
    rows  = conn2.execute("SELECT COUNT(*) FROM agora.main.silver_equity_ohlcv_daily").fetchone()[0]
    conn2.close()
    log.info(f"Final state: {total} symbols, {rows:,} rows")


if __name__ == "__main__":
    main()