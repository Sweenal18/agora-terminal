import requests
import duckdb
import logging
from dagster import asset

logger = logging.getLogger(__name__)

DUCKDB_PATH = "/app/transform/dbt/agora.duckdb"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

@asset(group_name="filings", description="SEC CIK mapping: ticker -> CIK number from SEC bulk file")
def silver_cik_mapping():
    headers = {
        "User-Agent": "Agora Terminal contact@agora-terminal.com",
        "Accept-Encoding": "gzip, deflate"
    }
    resp = requests.get(SEC_TICKERS_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for entry in data.values():
        cik_str = str(entry["cik_str"]).zfill(10)
        rows.append((entry["ticker"].upper(), cik_str, entry["title"]))

    logger.info(f"Fetched {len(rows)} CIK mappings from SEC")

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE OR REPLACE TABLE main.silver_cik_mapping (
            symbol VARCHAR,
            cik    VARCHAR,
            name   VARCHAR
        )
    """)
    con.executemany("INSERT INTO main.silver_cik_mapping VALUES (?, ?, ?)", rows)
    count = con.execute("SELECT COUNT(*) FROM main.silver_cik_mapping").fetchone()[0]
    con.close()
    logger.info(f"CIK mapping loaded: {count} companies")