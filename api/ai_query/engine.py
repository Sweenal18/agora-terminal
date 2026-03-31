"""
Agora Terminal - AI Query Engine
Natural language -> DuckDB SQL -> results
Model: qwen2.5-coder:3b via Ollama (local)
"""

import json
import re
import time
import urllib.request
import urllib.error
import duckdb
from typing import Any

OLLAMA_URL = "http://host.docker.internal:11434/api/generate"
DUCKDB_PATH = "/app/transform/dbt/agora.duckdb"
MODEL = "qwen2.5-coder:3b"

SCHEMA_CONTEXT = """
You have access to a DuckDB financial database. All tables are in the schema: agora.main_gold

TABLE: agora.main_gold.fct_prices
PURPOSE: Daily OHLCV price data for equities and crypto
COLUMNS:
  instrument_key VARCHAR  -- join key to dim_instruments
  date_key       INTEGER  -- join key to dim_time (format YYYYMMDD)
  symbol         VARCHAR  -- ticker e.g. AAPL, BTC-USD
  trade_date     DATE
  open           DOUBLE
  high           DOUBLE
  low            DOUBLE
  close          DOUBLE
  volume         BIGINT
  vwap           DOUBLE
  daily_return_pct DOUBLE  -- percentage daily return
  price_range    DOUBLE
  price_range_pct DOUBLE
  volume_usd_approx DOUBLE
  volume_ratio   DOUBLE    -- volume vs 30d average
  is_up_day      BOOLEAN
  asset_class    VARCHAR   -- equity or crypto
  adjusted       BOOLEAN
NOTE: fct_prices does NOT have price_to_book, roe, beta, market_cap or any fundamental metrics

TABLE: agora.main_gold.fct_fundamentals
PURPOSE: Fundamental metrics per instrument per snapshot date
USE THIS TABLE for: price_to_book, price_to_sales, roe, beta, market_cap, dividend_yield, ev_to_ebitda, roic, current_ratio, week_52_high, week_52_low
COLUMNS:
  instrument_key VARCHAR
  date_key       INTEGER
  symbol         VARCHAR
  snapshot_date  DATE
  market_cap     DOUBLE
  beta           DOUBLE
  week_52_high   DOUBLE
  week_52_low    DOUBLE
  roe            DOUBLE
  ev_to_ebitda   DOUBLE
  price_to_book  DOUBLE
  price_to_sales DOUBLE
  dividend_yield DOUBLE
  roic           DOUBLE
  current_ratio  DOUBLE
  sector         VARCHAR
  industry       VARCHAR

TABLE: agora.main_gold.fct_macro
PURPOSE: FRED macroeconomic indicators
COLUMNS:
  date_key          INTEGER
  series_id         VARCHAR
  observation_date  DATE
  series_name       VARCHAR
  unit              VARCHAR
  indicator_value   DOUBLE
  series_category   VARCHAR
  reporting_frequency VARCHAR

TABLE: agora.main_gold.dim_instruments
PURPOSE: Instrument master data
COLUMNS:
  instrument_key   VARCHAR
  symbol           VARCHAR
  company_name     VARCHAR
  sector           VARCHAR
  industry         VARCHAR
  currency         VARCHAR
  asset_class      VARCHAR
  market_cap       DOUBLE
  market_cap_bucket VARCHAR

TABLE: agora.main_gold.dim_time
PURPOSE: Date dimension
COLUMNS:
  date_key        INTEGER
  calendar_date   DATE
  year            BIGINT
  quarter         BIGINT
  month_number    BIGINT
  month_name      VARCHAR
  week_of_year    BIGINT
  day_of_week     BIGINT
  day_name        VARCHAR
  fiscal_year     BIGINT
  fiscal_quarter  BIGINT
  year_month      VARCHAR
  is_weekday      BOOLEAN
  is_trading_day  BOOLEAN
  is_last_7_days  BOOLEAN
  is_last_30_days BOOLEAN
  is_last_365_days BOOLEAN
  is_ytd          BOOLEAN
  is_today        BOOLEAN

IMPORTANT RULES:
- Always prefix tables with agora.main_gold.
- Join fct_prices to dim_instruments using instrument_key
- Join any fact table to dim_time using date_key to filter by date
- is_last_30_days, is_ytd, is_last_7_days, is_today ONLY exist on dim_time -- NEVER use these on fact tables directly
- DuckDB date arithmetic: use CURRENT_DATE - INTERVAL '30 days' NOT DATE_SUB(). Example: WHERE trade_date >= CURRENT_DATE - INTERVAL '30 days'
- Prefer using dim_time boolean flags (is_last_30_days, is_ytd) over date arithmetic when possible
- DuckDB does NOT support tuple IN subquery syntax like (col1, col2) IN (SELECT ...) -- never use this
- For latest value per series in fct_macro use JOIN pattern:
  JOIN (SELECT series_id, MAX(observation_date) AS max_date FROM agora.main_gold.fct_macro GROUP BY series_id) latest
  ON m.series_id = latest.series_id AND m.observation_date = latest.max_date
- For fundamental metrics (price_to_book, roe, beta etc.) always use fct_fundamentals, never fct_prices
- For GDP, inflation, employment, yield curve, VIX questions always use fct_macro, never fct_fundamentals
- fct_fundamentals does NOT have series_id, GDP, or any macro columns -- it only has the columns listed above
- In fct_fundamentals, roe and roic and dividend_yield are stored as DECIMALS not percentages. roe=0.20 means 20%. So "ROE above 20%" means WHERE roe > 0.20, NOT WHERE roe > 20
- Available series_id values in fct_macro: T10Y2Y (yield curve), T10Y3M (yield curve), CPIAUCSL (inflation CPI), PCEPI (inflation PCE), PAYEMS (nonfarm payrolls), GDP (nominal GDP), GDPC1 (real GDP), INDPRO (industrial production), VIXCLS (VIX volatility index). Never use shorthand like VIX, CPI, GDP_GROWTH -- use exact series_id values only
- For sector filtering on fundamentals, fct_fundamentals has sector column directly -- no need to join dim_instruments
- When using fct_fundamentals with alias f, always use f.column_name in SELECT and WHERE -- never use p.column_name
- Always add LIMIT 100 unless user asks for all data
- Return only the SQL query, no explanation, no markdown fences
"""

SYSTEM_PROMPT = """You are a financial data analyst assistant for Agora Terminal.
Your job is to convert natural language questions into DuckDB SQL queries.

""" + SCHEMA_CONTEXT + """

Rules:
- Return ONLY the SQL query, nothing else
- No markdown code fences
- No explanations before or after
- Use proper DuckDB syntax
- Always use the full table path: agora.main_gold.<table_name>
"""

# Map of table short names to their canonical alias
TABLE_ALIAS_MAP = {
    "fct_prices": "p",
    "fct_fundamentals": "f",
    "fct_macro": "m",
    "dim_instruments": "i",
    "dim_time": "dt",
}

# Columns that belong exclusively to each table
TABLE_COLUMNS = {
    "fct_fundamentals": {
        "price_to_book", "price_to_sales", "roe", "beta", "market_cap",
        "dividend_yield", "ev_to_ebitda", "roic", "current_ratio",
        "week_52_high", "week_52_low", "snapshot_date",
    },
    "fct_prices": {
        "open", "high", "low", "close", "volume", "vwap",
        "daily_return_pct", "price_range", "price_range_pct",
        "volume_usd_approx", "volume_ratio", "is_up_day", "trade_date",
    },
}


def _fix_alias(sql: str) -> str:
    """
    Detect the actual alias used for each table in FROM/JOIN clauses
    and fix any stale aliases (e.g. p. used instead of f.) in SELECT/WHERE.
    """
    # Extract actual aliases: pattern is "agora.main_gold.TABLE_NAME alias"
    actual_aliases: dict[str, str] = {}
    for table, canonical in TABLE_ALIAS_MAP.items():
        pattern = rf"agora\.main_gold\.{table}\s+(\w+)"
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            actual_aliases[table] = match.group(1)

    # For each table, if its columns are prefixed with wrong alias, fix them
    for table, cols in TABLE_COLUMNS.items():
        if table not in actual_aliases:
            continue
        correct_alias = actual_aliases[table]
        for wrong_alias in set(TABLE_ALIAS_MAP.values()) - {correct_alias}:
            for col in cols:
                # Replace wrong_alias.col with correct_alias.col
                sql = re.sub(
                    rf"\b{wrong_alias}\.{col}\b",
                    f"{correct_alias}.{col}",
                    sql,
                    flags=re.IGNORECASE,
                )
    return sql


def _call_ollama(question: str) -> str:
    payload = {
        "model": MODEL,
        "prompt": SYSTEM_PROMPT + "\n\nQuestion: " + question + "\n\nSQL:",
        "stream": False,
        "options": {
            "temperature": 0.1,
            "num_predict": 512,
        }
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        OLLAMA_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read().decode())
        return result["response"].strip()


def _clean_sql(raw: str) -> str:
    raw = re.sub(r"```(?:sql)?", "", raw, flags=re.IGNORECASE).strip()
    raw = raw.strip("`").strip()
    select_match = re.search(r"\bSELECT\b", raw, re.IGNORECASE)
    if select_match:
        raw = raw[select_match.start():]
    raw = _fix_alias(raw)
    return raw.strip()


def _execute_sql(sql: str) -> list[dict[str, Any]]:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        result = con.execute(sql).fetchdf()
        return result.to_dict(orient="records")
    finally:
        con.close()


def run_query(question: str) -> dict[str, Any]:
    start = time.time()
    sql = None
    try:
        raw_sql = _call_ollama(question)
        sql = _clean_sql(raw_sql)
        results = _execute_sql(sql)
        duration_ms = int((time.time() - start) * 1000)
        return {
            "question": question,
            "sql": sql,
            "results": results,
            "row_count": len(results),
            "duration_ms": duration_ms,
            "error": None,
        }
    except urllib.error.URLError as e:
        return {
            "question": question,
            "sql": sql,
            "results": [],
            "row_count": 0,
            "duration_ms": int((time.time() - start) * 1000),
            "error": "Ollama unreachable: " + str(e),
        }
    except duckdb.Error as e:
        return {
            "question": question,
            "sql": sql,
            "results": [],
            "row_count": 0,
            "duration_ms": int((time.time() - start) * 1000),
            "error": "SQL execution error: " + str(e),
        }
    except Exception as e:
        return {
            "question": question,
            "sql": sql,
            "results": [],
            "row_count": 0,
            "duration_ms": int((time.time() - start) * 1000),
            "error": "Unexpected error: " + str(e),
        }