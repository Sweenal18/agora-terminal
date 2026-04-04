"""
Macro asset -- fetches FRED data and stores key indicators
"""
import os
import requests
import duckdb
from dagster import asset, AssetExecutionContext, RetryPolicy, Backoff

FRED_API_KEY = os.getenv("FRED_API_KEY", "")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/transform/dbt/agora.duckdb")
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

FRED_SERIES = {
    "fed_rate":     "FEDFUNDS",
    "treasury_10y": "GS10",
    "cpi":          "CPIAUCSL",
    "unemployment": "UNRATE",
    "gdp":          "GDP",
}

def fetch_fred(series_id: str) -> dict:
    try:
        res = requests.get(FRED_BASE, params={
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 1,
        }, timeout=15)
        data = res.json()
        obs = data.get("observations", [])
        if obs:
            return {"value": obs[0]["value"], "date": obs[0]["date"]}
    except Exception:
        pass
    return {"value": None, "date": None}

@asset(
    group_name="macro",
    description="Fetch latest FRED macro indicators and store in DuckDB",
    retry_policy=RetryPolicy(max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL),
)
def silver_macro_pulse(context: AssetExecutionContext):
    """Fetch and store FRED macro indicators."""
    if not FRED_API_KEY:
        raise ValueError("FRED_API_KEY not set")

    results = {}
    for key, series_id in FRED_SERIES.items():
        context.log.info(f"Fetching FRED series: {series_id}")
        results[key] = fetch_fred(series_id)

    conn = duckdb.connect(DUCKDB_PATH)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agora.main.silver_macro_pulse (
                indicator VARCHAR,
                value VARCHAR,
                date VARCHAR,
                fetched_at TIMESTAMP
            )
        """)
        conn.execute("DELETE FROM agora.main.silver_macro_pulse")
        for key, data in results.items():
            conn.execute(
                "INSERT INTO agora.main.silver_macro_pulse VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                [key, data["value"], data["date"]]
            )
    finally:
        conn.close()

    context.log.info(f"Stored {len(results)} macro indicators")
    return results