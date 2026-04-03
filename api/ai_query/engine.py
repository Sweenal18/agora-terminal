"""
Agora Terminal - AI Query Engine
Natural language -> DuckDB SQL -> results
Model: llama-3.1-8b-instant via Groq API
"""

import json
import os
import re
import time
import urllib.request
import urllib.error
import duckdb
from typing import Any

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
DUCKDB_PATH = "/app/transform/dbt/agora.duckdb"
MODEL = "llama-3.1-8b-instant"

SCHEMA_CONTEXT = """
You have access to a DuckDB financial database. All tables are in the schema: agora.main_gold

TABLE: agora.main_gold.fct_prices
PURPOSE: Daily OHLCV price data for equities and crypto
COLUMNS:
  instrument_key VARCHAR
  date_key       INTEGER
  symbol         VARCHAR
  trade_date     DATE
  open           DOUBLE
  high           DOUBLE
  low            DOUBLE
  close          DOUBLE
  volume         BIGINT
  vwap           DOUBLE
  daily_return_pct DOUBLE
  price_range_pct DOUBLE
  volume_usd_approx DOUBLE
  volume_ratio   DOUBLE
  is_up_day      BOOLEAN
  asset_class    VARCHAR
NOTE: fct_prices does NOT have company_name, price_to_book, roe, beta, market_cap or any fundamental metrics

TABLE: agora.main_gold.fct_fundamentals
PURPOSE: Fundamental metrics per instrument per snapshot date
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
NOTE: fct_fundamentals does NOT have company_name

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

TABLE: agora.main_gold.dim_instruments
PURPOSE: Instrument master data - the ONLY table with company_name
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
  month_number    BIGINT
  is_weekday      BOOLEAN
  is_trading_day  BOOLEAN
  is_last_7_days  BOOLEAN
  is_last_30_days BOOLEAN
  is_last_365_days BOOLEAN
  is_ytd          BOOLEAN
  is_today        BOOLEAN

CRITICAL RULES:
- Always prefix tables with agora.main_gold.
- company_name ONLY exists in dim_instruments -- NEVER select company_name from fct_prices or fct_fundamentals
- Always use these aliases: fct_prices=p, fct_fundamentals=f, dim_instruments=d, fct_macro=m, dim_time=dt
- Always JOIN dim_instruments d ON p.instrument_key = d.instrument_key to get d.symbol and d.company_name
- Join fact tables to dim_time using date_key to filter by date
- is_last_30_days, is_ytd, is_last_7_days ONLY exist on dim_time -- never on fact tables
- To use dim_time flags you MUST join it: JOIN agora.main_gold.dim_time dt ON p.date_key = dt.date_key THEN use dt.is_last_30_days = true
- Alternatively use date arithmetic directly: WHERE p.trade_date >= CURRENT_DATE - INTERVAL '30 days' (this is simpler and preferred)
- DuckDB date arithmetic: use CURRENT_DATE - INTERVAL '30 days' NOT DATE_SUB()
- For fundamental metrics (price_to_book, roe, beta etc.) use fct_fundamentals
- For GDP, inflation, employment, VIX use fct_macro with exact series_id values:
  T10Y2Y, T10Y3M, CPIAUCSL, PCEPI, PAYEMS, GDP, GDPC1, INDPRO, VIXCLS
- roe, roic, dividend_yield in fct_fundamentals are decimals: roe=0.20 means 20%
- For latest macro value per series use:
  JOIN (SELECT series_id, MAX(observation_date) AS max_date FROM agora.main_gold.fct_macro GROUP BY series_id) latest
  ON m.series_id = latest.series_id AND m.observation_date = latest.max_date
- If question mentions a specific stock symbol like AAPL, add WHERE d.symbol = 'AAPL'
- Always add LIMIT 50 unless user asks for all data
- Return ONLY the SQL query, no explanation, no markdown fences
"""

SYSTEM_PROMPT = """You are a financial data analyst for Agora Terminal.
Convert natural language questions into DuckDB SQL queries.

""" + SCHEMA_CONTEXT


TABLE_ALIAS_MAP = {
    "fct_prices": "p",
    "fct_fundamentals": "f",
    "fct_macro": "m",
    "dim_instruments": "d",
    "dim_time": "dt",
}

TABLE_COLUMNS = {
    "fct_fundamentals": {
        "price_to_book", "price_to_sales", "roe", "beta", "market_cap",
        "dividend_yield", "ev_to_ebitda", "roic", "current_ratio",
        "week_52_high", "week_52_low", "snapshot_date",
    },
    "fct_prices": {
        "open", "high", "low", "close", "volume", "vwap",
        "daily_return_pct", "price_range_pct",
        "volume_usd_approx", "volume_ratio", "is_up_day", "trade_date",
    },
}


def _fix_alias(sql: str) -> str:
    actual_aliases: dict[str, str] = {}
    for table, canonical in TABLE_ALIAS_MAP.items():
        pattern = rf"agora\.main_gold\.{table}\s+(\w+)"
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            actual_aliases[table] = match.group(1)
    for table, cols in TABLE_COLUMNS.items():
        if table not in actual_aliases:
            continue
        correct_alias = actual_aliases[table]
        for wrong_alias in set(TABLE_ALIAS_MAP.values()) - {correct_alias}:
            for col in cols:
                sql = re.sub(
                    rf"\b{wrong_alias}\.{col}\b",
                    f"{correct_alias}.{col}",
                    sql,
                    flags=re.IGNORECASE,
                )
    return sql


def _call_groq(question: str) -> str:
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": "Question: " + question + "\n\nSQL:"}
        ],
        "temperature": 0.1,
        "max_tokens": 512,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        GROQ_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer " + GROQ_API_KEY,
        },
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read().decode())
        return result["choices"][0]["message"]["content"].strip()


def _clean_sql(raw: str) -> str:
    raw = re.sub(r"```(?:sql)?", "", raw, flags=re.IGNORECASE).strip()
    raw = raw.strip("`").strip()
    select_match = re.search(r"\bSELECT\b", raw, re.IGNORECASE)
    if select_match:
        raw = raw[select_match.start():]
    raw = _fix_alias(raw)
    raw = re.sub(
        r"DATE_SUB\s*\(\s*CURRENT_DATE\s*,\s*(INTERVAL\s+['\"]?[0-9]+\s+\w+['\"]?)\s*\)",
        r"CURRENT_DATE - \1",
        raw, flags=re.IGNORECASE
    )
    # Fix model incorrectly placing dim_time flags on fact table aliases
    import re as _re2
    raw = _re2.sub(r'\b\w+\.is_last_30_days\s*=\s*true', "p.trade_date >= CURRENT_DATE - INTERVAL '30 days'", raw, flags=_re2.IGNORECASE)
    raw = _re2.sub(r'\b\w+\.is_last_7_days\s*=\s*true', "p.trade_date >= CURRENT_DATE - INTERVAL '7 days'", raw, flags=_re2.IGNORECASE)
    raw = _re2.sub(r'\b\w+\.is_ytd\s*=\s*true', "EXTRACT(year FROM p.trade_date) = EXTRACT(year FROM CURRENT_DATE)", raw, flags=_re2.IGNORECASE)
    raw = _re2.sub(r'\b\w+\.is_last_365_days\s*=\s*true', "p.trade_date >= CURRENT_DATE - INTERVAL '365 days'", raw, flags=_re2.IGNORECASE)
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
        raw_sql = _call_groq(question)
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
            "error": "Groq API error: " + str(e),
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