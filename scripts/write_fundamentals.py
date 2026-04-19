path = r"C:\Projects\agora-terminal\agora-terminal\orchestration\dagster\assets\fundamentals.py"
content = '''"""
Fundamentals asset -- fetches Finnhub data for S&P 500 and writes to DuckDB silver layer.
Switched from FMP (250 calls/day cap) to Finnhub (60 calls/min, no daily cap).
"""
import os
import time
import logging
import requests
import duckdb
from datetime import datetime, timezone
from dagster import asset, AssetExecutionContext

log = logging.getLogger("dagster.fundamentals")

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
FINNHUB_BASE    = "https://finnhub.io/api/v1"
DUCKDB_PATH     = os.getenv("DUCKDB_PATH", "/app/transform/dbt/agora.duckdb")

HEADERS = {"X-Finnhub-Token": FINNHUB_API_KEY}


def safe_float(val, default=None):
    try:
        if val is None or val == "":
            return default
        return float(val)
    except Exception:
        return default


def safe_int(val, default=None):
    try:
        if val is None or val == "":
            return default
        return int(val)
    except Exception:
        return default


def fetch_finnhub(endpoint: str, params: dict) -> dict:
    url = f"{FINNHUB_BASE}/{endpoint}"
    for attempt in range(3):
        try:
            res = requests.get(url, headers={"X-Finnhub-Token": FINNHUB_API_KEY}, params=params, timeout=15)
            if res.status_code == 429:
                wait = 60 * (attempt + 1)
                log.warning(f"Finnhub 429 at {endpoint}, waiting {wait}s (attempt {attempt+1}/3)")
                time.sleep(wait)
                continue
            res.raise_for_status()
            return res.json()
        except Exception as e:
            log.warning(f"Finnhub error at {endpoint}: {e}")
            if attempt < 2:
                time.sleep(10)
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
        safe_int(data.get("avg_volume")),
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
        safe_int(data.get("employees")),
        data.get("website"),
        datetime.now(timezone.utc),
    ])


@asset(
    group_name="fundamentals",
    description="Fetch Finnhub fundamentals for S&P 500 and upsert into DuckDB silver layer",
)
def silver_equity_fundamentals(context: AssetExecutionContext):
    """Fetch and store equity fundamentals from Finnhub API."""
    if not FINNHUB_API_KEY:
        raise ValueError("FINNHUB_API_KEY not set -- set it in infra/docker/.env")

    conn = duckdb.connect(DUCKDB_PATH)
    create_table(conn)
    success = 0

    # Load full symbol universe -- Finnhub has no daily cap, 60 calls/min
    # 2 endpoints per symbol = ~17 min for 502 symbols at 1s sleep
    try:
        SYMBOLS = [r[0] for r in conn.execute("""
            SELECT DISTINCT symbol
            FROM main.silver_equity_ohlcv_daily
            ORDER BY symbol
        """).fetchall()]
        context.log.info(f"Loaded {len(SYMBOLS)} symbols from silver_equity_ohlcv_daily")
    except Exception as e:
        context.log.warning(f"Could not load symbols from DB: {e}. Falling back to empty list.")
        SYMBOLS = []

    for i, symbol in enumerate(SYMBOLS):
        context.log.info(f"[{i+1}/{len(SYMBOLS)}] Fetching {symbol}")

        # /stock/profile2 -- company info
        profile = fetch_finnhub("stock/profile2", {"symbol": symbol})
        # /stock/metric?metric=all -- all ratios and metrics
        metrics_resp = fetch_finnhub("stock/metric", {"symbol": symbol, "metric": "all"})
        metrics = metrics_resp.get("metric", {})

        if not profile or not profile.get("name"):
            context.log.warning(f"No profile for {symbol}, skipping")
            time.sleep(1.0)
            continue

        # Finnhub market cap is in millions -- convert to full value
        market_cap_raw = safe_float(profile.get("marketCapitalization"))
        market_cap = market_cap_raw * 1_000_000 if market_cap_raw else None

        upsert(conn, {
            "symbol":               symbol,
            "company_name":         profile.get("name"),
            "sector":               profile.get("finnhubIndustry"),
            "industry":             profile.get("finnhubIndustry"),
            "exchange":             profile.get("exchange"),
            "country":              profile.get("country"),
            "market_cap":           market_cap,
            "beta":                 metrics.get("beta"),
            "avg_volume":           metrics.get("10DayAverageTradingVolume"),
            "week_52_high":         metrics.get("52WeekHigh"),
            "week_52_low":          metrics.get("52WeekLow"),
            "price_to_sales":       metrics.get("psTTM"),
            "price_to_book":        metrics.get("pbQuarterly"),
            "dividend_yield":       metrics.get("dividendYieldIndicatedAnnual"),
            "roe":                  metrics.get("roeTTM"),
            "roic":                 metrics.get("roicTTM"),
            "ev_to_ebitda":         metrics.get("currentEv/freeCashFlowTTM"),
            "current_ratio":        metrics.get("currentRatioQuarterly"),
            "debt_to_equity":       metrics.get("totalDebt/totalEquityQuarterly"),
            "free_cash_flow_yield": metrics.get("freeCashFlowYieldTTM"),
            "description":          profile.get("name"),
            "logo_url":             profile.get("logo"),
            "ceo":                  None,
            "employees":            safe_int(profile.get("employeeTotal")),
            "website":              profile.get("weburl"),
        })
        success += 1
        context.log.info(f"  Saved {symbol} -- {profile.get('name')} ({profile.get('finnhubIndustry')})")
        time.sleep(1.0)  # 60 calls/min limit -- 1s sleep keeps us at ~2 calls/s (profile + metrics)

    conn.close()
    context.log.info(f"Done. {success}/{len(SYMBOLS)} symbols saved")
    return {"records_written": success}
'''

with open(path, "w", encoding="utf-8", newline="\n") as f:
    f.write(content)
print("Written successfully")
