"""
Fundamentals asset -- fetches FMP data for S&P 500 and writes to DuckDB silver layer.
Schema matches ingestion/fetchers/fundamentals.py exactly.
"""
import os
import time
import logging
import requests
import duckdb
from datetime import datetime, timezone
from dagster import asset, AssetExecutionContext

log = logging.getLogger("dagster.fundamentals")

FMP_API_KEY  = os.getenv("FMP_API_KEY", "")
FMP_BASE     = "https://financialmodelingprep.com/stable"
DUCKDB_PATH  = os.getenv("DUCKDB_PATH", "/app/transform/dbt/agora.duckdb")

# Symbols are loaded dynamically from the database at runtime

HEADERS = {"User-Agent": "Mozilla/5.0"}

def safe_float(val, default=None):
    try:
        if val is None or val == "":
            return default
        return float(val)
    except Exception:
        return default

def fetch_fmp(endpoint: str, ticker: str) -> dict:
    url = f"{FMP_BASE}/{endpoint}?symbol={ticker}&apikey={FMP_API_KEY}"
    for attempt in range(4):
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
            if res.status_code == 429:
                wait = 60 * (attempt + 1)
                log.warning(f"FMP 429 for {ticker} at {endpoint}, waiting {wait}s (attempt {attempt+1}/4)")
                time.sleep(wait)
                continue
            res.raise_for_status()
            data = res.json()
            if isinstance(data, dict) and "value" in data:
                data = data["value"]
            if isinstance(data, list) and data:
                return data[0]
            if isinstance(data, dict):
                return data
            return {}
        except Exception as e:
            log.warning(f"FMP error for {ticker} at {endpoint}: {e}")
            if attempt < 3:
                time.sleep(30)
    return {}

def create_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver_equity_fundamentals (
            symbol               VARCHAR PRIMARY KEY,
            company_name         VARCHAR,
            sector               VARCHAR,
            industry             VARCHAR,
            exchange             VARCHAR,
            country              VARCHAR,
            market_cap           DOUBLE,
            beta                 DOUBLE,
            avg_volume           BIGINT,
            week_52_high         DOUBLE,
            week_52_low          DOUBLE,
            price_to_sales       DOUBLE,
            price_to_book        DOUBLE,
            dividend_yield       DOUBLE,
            roe                  DOUBLE,
            roic                 DOUBLE,
            ev_to_ebitda         DOUBLE,
            current_ratio        DOUBLE,
            debt_to_equity       DOUBLE,
            free_cash_flow_yield DOUBLE,
            description          VARCHAR,
            logo_url             VARCHAR,
            ceo                  VARCHAR,
            employees            INTEGER,
            website              VARCHAR,
            updated_at           TIMESTAMP
        )
    """)

def upsert(conn, data: dict):
    conn.execute("""
        INSERT OR REPLACE INTO silver_equity_fundamentals VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
    """, [
        data.get("symbol"),
        data.get("company_name"),
        data.get("sector"),
        data.get("industry"),
        data.get("exchange"),
        data.get("country"),
        safe_float(data.get("market_cap")),
        safe_float(data.get("beta")),
        data.get("avg_volume"),
        safe_float(data.get("week_52_high")),
        safe_float(data.get("week_52_low")),
        safe_float(data.get("price_to_sales")),
        safe_float(data.get("price_to_book")),
        safe_float(data.get("dividend_yield")),
        safe_float(data.get("roe")),
        safe_float(data.get("roic")),
        safe_float(data.get("ev_to_ebitda")),
        safe_float(data.get("current_ratio")),
        safe_float(data.get("debt_to_equity")),
        safe_float(data.get("free_cash_flow_yield")),
        data.get("description"),
        data.get("logo_url"),
        data.get("ceo"),
        data.get("employees"),
        data.get("website"),
        datetime.now(timezone.utc),
    ])

@asset(
    group_name="fundamentals",
    description="Fetch FMP fundamentals for S&P 500 and upsert into DuckDB silver layer",
)
def silver_equity_fundamentals(context: AssetExecutionContext):
    """Fetch and store equity fundamentals from FMP API."""
    if not FMP_API_KEY:
        raise ValueError("FMP_API_KEY not set — set it in infra/docker/.env")

    conn = duckdb.connect(DUCKDB_PATH)
    create_table(conn)
    success = 0

    # Load symbol universe -- rotate through stalest symbols first (FMP free tier: 250 calls/day)
    # 80 symbols x 3 endpoints = 240 calls, leaving buffer for retries
    DAILY_LIMIT = 80
    try:
        # Left join to get symbols with no fundamentals first, then stalest updated_at
        SYMBOLS = [r[0] for r in conn.execute("""
            SELECT s.symbol
            FROM (SELECT DISTINCT symbol FROM main.silver_equity_ohlcv_daily) s
            LEFT JOIN silver_equity_fundamentals f ON s.symbol = f.symbol
            ORDER BY f.updated_at ASC NULLS FIRST
            LIMIT ?
        """, [DAILY_LIMIT]).fetchall()]
        context.log.info(f"Loaded {len(SYMBOLS)} stalest symbols for today's run (limit {DAILY_LIMIT})")
    except Exception as e:
        context.log.warning(f"Could not load symbols from DB: {e}. Falling back to empty list.")
        SYMBOLS = []

    for i, symbol in enumerate(SYMBOLS):
        context.log.info(f"[{i+1}/{len(SYMBOLS)}] Fetching {symbol}")
        profile = fetch_fmp("profile", symbol)
        ratios  = fetch_fmp("ratios",  symbol)
        metrics = fetch_fmp("key-metrics", symbol)

        if not profile:
            context.log.warning(f"No profile for {symbol}, skipping")
            time.sleep(1.0)
            continue

        range_str = profile.get("range", "-")
        week_52_low, week_52_high = None, None
        if "-" in str(range_str):
            parts = str(range_str).split("-")
            if len(parts) == 2:
                week_52_low  = safe_float(parts[0].strip())
                week_52_high = safe_float(parts[1].strip())

        upsert(conn, {
            "symbol":             symbol,
            "company_name":       profile.get("companyName"),
            "sector":             profile.get("sector"),
            "industry":           profile.get("industry"),
            "exchange":           profile.get("exchange"),
            "country":            profile.get("country"),
            "market_cap":         profile.get("marketCap"),
            "beta":               profile.get("beta"),
            "avg_volume":         profile.get("averageVolume"),
            "week_52_high":       week_52_high,
            "week_52_low":        week_52_low,
            "price_to_sales":     ratios.get("priceToSalesRatio"),
            "price_to_book":      ratios.get("priceToBookRatio"),
            "dividend_yield":     ratios.get("dividendYield"),
            "roe":                metrics.get("returnOnEquity"),
            "roic":               metrics.get("returnOnInvestedCapital"),
            "ev_to_ebitda":       metrics.get("evToEBITDA"),
            "current_ratio":      metrics.get("currentRatio"),
            "debt_to_equity":     ratios.get("debtToEquity"),
            "free_cash_flow_yield": metrics.get("freeCashFlowYield"),
            "description":        profile.get("description"),
            "logo_url":           profile.get("image"),
            "ceo":                profile.get("ceo"),
            "employees":          profile.get("fullTimeEmployees"),
            "website":            profile.get("website"),
        })
        success += 1
        context.log.info(f"  Saved {symbol} -- {profile.get('companyName')} ({profile.get('sector')})")
        time.sleep(1.0)

    conn.close()
    context.log.info(f"Done. {success}/{len(SYMBOLS)} symbols saved")
    return {"records_written": success}