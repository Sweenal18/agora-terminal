"""
Agora Terminal - FRED Macro Indicators Fetcher
Fetches historical data for 15 FRED series and writes to silver_macro_indicators.
Run locally (not in Docker) then docker cp DuckDB into container.

Usage: python ingestion/fetchers/fetch_macro_indicators.py
"""

import time
import requests
import duckdb
from datetime import datetime

FRED_API_KEY = "e73fff17cb8ccc80636b2d3612c953ef"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
DUCKDB_PATH = "transform/dbt/agora.duckdb"

# 15 series covering all fct_macro categories
SERIES = {
    # interest_rate
    "FEDFUNDS":       "Federal Funds Effective Rate",
    "DFF":            "Federal Funds Rate (Daily)",
    # yield_curve
    "T10Y2Y":         "10-Year minus 2-Year Treasury Spread",
    "T10Y3M":         "10-Year minus 3-Month Treasury Spread",
    "T5Y5E":          "5-Year 5-Year Forward Inflation Expectation",
    # inflation
    "CPIAUCSL":       "Consumer Price Index (All Urban)",
    "CPILFESL":       "Core CPI (Less Food and Energy)",
    "PCEPI":          "PCE Price Index",
    # employment
    "UNRATE":         "Unemployment Rate",
    "ICSA":           "Initial Jobless Claims",
    "PAYEMS":         "Total Nonfarm Payrolls",
    # growth
    "GDP":            "Gross Domestic Product",
    "GDPC1":          "Real GDP",
    "INDPRO":         "Industrial Production Index",
    # risk_sentiment
    "VIXCLS":         "CBOE Volatility Index (VIX)",
}

UNITS = {
    "FEDFUNDS": "percent",
    "DFF":      "percent",
    "T10Y2Y":   "percent",
    "T10Y3M":   "percent",
    "T5Y5E":    "percent",
    "CPIAUCSL": "index_1982_84_100",
    "CPILFESL": "index_1982_84_100",
    "PCEPI":    "index_2012_100",
    "UNRATE":   "percent",
    "ICSA":     "thousands",
    "PAYEMS":   "thousands",
    "GDP":      "billions_usd",
    "GDPC1":    "billions_2017_usd",
    "INDPRO":   "index_2017_100",
    "VIXCLS":   "index",
}


def fetch_series(series_id: str, series_name: str) -> list[dict]:
    """Fetch full history (10 years) for a FRED series."""
    print(f"  Fetching {series_id} - {series_name}...", end=" ")
    try:
        resp = requests.get(FRED_BASE, params={
            "series_id":        series_id,
            "api_key":          FRED_API_KEY,
            "file_type":        "json",
            "sort_order":       "asc",
            "observation_start": "2015-01-01",
            "limit":            10000,
        }, timeout=30)
        data = resp.json()
        observations = data.get("observations", [])

        rows = []
        for obs in observations:
            val = obs.get("value", ".")
            if val == "." or val is None:
                continue  # skip missing values
            try:
                rows.append({
                    "series_id":        series_id,
                    "series_name":      series_name,
                    "value":            float(val),
                    "observation_date": obs["date"],
                    "unit":             UNITS.get(series_id, "unknown"),
                    "processed_at":     datetime.utcnow().isoformat(),
                })
            except (ValueError, TypeError):
                continue

        print(f"{len(rows)} observations")
        return rows

    except Exception as e:
        print(f"ERROR: {e}")
        return []


def write_to_duckdb(all_rows: list[dict]):
    """Write all rows to silver_macro_indicators in DuckDB."""
    print(f"\nWriting {len(all_rows)} rows to DuckDB...")
    con = duckdb.connect(DUCKDB_PATH)
    try:
        # Ensure table exists with correct schema
        con.execute("""
            CREATE TABLE IF NOT EXISTS agora.main.silver_macro_indicators (
                series_id        VARCHAR,
                series_name      VARCHAR,
                value            DOUBLE,
                observation_date DATE,
                unit             VARCHAR,
                processed_at     TIMESTAMP
            )
        """)

        # Clear existing data and reload fresh
        con.execute("DELETE FROM agora.main.silver_macro_indicators")

        # Bulk insert
        con.executemany("""
            INSERT INTO agora.main.silver_macro_indicators
            (series_id, series_name, value, observation_date, unit, processed_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            (
                r["series_id"],
                r["series_name"],
                r["value"],
                r["observation_date"],
                r["unit"],
                r["processed_at"],
            )
            for r in all_rows
        ])

        count = con.execute("SELECT COUNT(*) FROM agora.main.silver_macro_indicators").fetchone()[0]
        print(f"silver_macro_indicators now has {count} rows")

        # Show sample
        print("\nSample rows:")
        rows = con.execute("""
            SELECT series_id, observation_date, value
            FROM agora.main.silver_macro_indicators
            ORDER BY observation_date DESC
            LIMIT 5
        """).fetchall()
        for r in rows:
            print(f"  {r[0]} | {r[1]} | {r[2]}")

    finally:
        con.close()


def main():
    print("=== Agora Terminal - FRED Macro Fetcher ===")
    print(f"Fetching {len(SERIES)} series from FRED (2015-present)...\n")

    all_rows = []
    for series_id, series_name in SERIES.items():
        rows = fetch_series(series_id, series_name)
        all_rows.extend(rows)
        time.sleep(0.3)  # be polite to FRED API

    print(f"\nTotal observations fetched: {len(all_rows)}")
    write_to_duckdb(all_rows)
    print("\nDone. Next step: docker cp the DuckDB file and run dbt.")


if __name__ == "__main__":
    main()