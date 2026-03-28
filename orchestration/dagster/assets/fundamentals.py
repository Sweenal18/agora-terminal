"""
Fundamentals asset — fetches FMP data for S&P 500 and writes to DuckDB
"""
import os
import time
import logging
import requests
import duckdb
from dagster import asset, AssetExecutionContext

log = logging.getLogger("dagster.fundamentals")

FMP_API_KEY = os.getenv("FMP_API_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/stable"
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/transform/dbt/agora.duckdb")

SP500_TICKERS = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","BRK-B","LLY","JPM","V",
    "XOM","UNH","JNJ","PG","MA","AVGO","HD","MRK","COST","CVX",
    "ABBV","WMT","BAC","CSCO","CRM","MCD","PEP","TMO","ACN","LIN",
    "AMD","INTC","QCOM","TXN","AMAT","NFLX","TSLA","ADBE","NOW","INTU",
    "DE","CAT","GE","HON","UPS","GS","MS","BLK","SPGI","CB",
]

HEADERS = {"User-Agent": "Mozilla/5.0"}

def fetch_fmp(endpoint: str, ticker: str) -> dict:
    url = f"{FMP_BASE}/{endpoint}?symbol={ticker}&apikey={FMP_API_KEY}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        res.raise_for_status()
        data = res.json()
        if isinstance(data, list) and data:
            return data[0]
        if isinstance(data, dict):
            return data
    except Exception as e:
        log.warning(f"FMP error for {ticker} at {endpoint}: {e}")
    return {}

@asset(
    group_name="fundamentals",
    description="Fetch FMP fundamentals for S&P 500 and write to DuckDB silver layer",
)
def silver_equity_fundamentals(context: AssetExecutionContext):
    """Fetch and store equity fundamentals from FMP API."""
    if not FMP_API_KEY:
        raise ValueError("FMP_API_KEY not set")

    records = []
    for i, ticker in enumerate(SP500_TICKERS):
        context.log.info(f"[{i+1}/{len(SP500_TICKERS)}] Fetching {ticker}")
        profile = fetch_fmp("profile", ticker)
        ratios = fetch_fmp("ratios", ticker)
        metrics = fetch_fmp("key-metrics", ticker)
        time.sleep(0.5)

        records.append({
            "symbol": ticker,
            "company_name": profile.get("companyName"),
            "sector": profile.get("sector"),
            "industry": profile.get("industry"),
            "market_cap": profile.get("mktCap"),
            "beta": profile.get("beta"),
            "week_52_high": profile.get("range", "0-0").split("-")[-1] if profile.get("range") else None,
            "week_52_low": profile.get("range", "0-0").split("-")[0] if profile.get("range") else None,
            "roe": ratios.get("returnOnEquity"),
            "ev_to_ebitda": ratios.get("enterpriseValueMultiple"),
            "price_to_book": ratios.get("priceToBookRatio"),
            "price_to_sales": ratios.get("priceToSalesRatio"),
            "dividend_yield": ratios.get("dividendYield"),
            "roic": metrics.get("roic"),
            "current_ratio": ratios.get("currentRatio"),
            "logo_url": profile.get("image"),
            "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        })

    import pandas as pd
    records_df = pd.DataFrame(records)
    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute("DROP TABLE IF EXISTS agora.main.silver_equity_fundamentals")
    conn.execute("CREATE TABLE agora.main.silver_equity_fundamentals AS SELECT * FROM records_df")
    conn.close()

    context.log.info(f"Wrote {len(records)} fundamentals records to DuckDB")
    return {"records_written": len(records)}