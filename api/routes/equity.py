"""
Equity routes — serves S&P 500 OHLCV data from DuckDB Silver layer
"""
import os
import logging
import requests
from fastapi import APIRouter
import duckdb
import time
import threading

log = logging.getLogger("api.equity")
router = APIRouter()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/transform/dbt/agora.duckdb")


def get_duckdb():
    return duckdb.connect(DUCKDB_PATH, read_only=True)

_cache = {}
_cache_lock = threading.Lock()

def ttl_cache_get(key):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() < entry["expires"]:
            return entry["value"]
    return None

def ttl_cache_set(key, value, ttl_seconds):
    with _cache_lock:
        _cache[key] = {"value": value, "expires": time.time() + ttl_seconds}


@router.get("/ohlcv/{symbol}")
def get_equity_ohlcv(symbol: str, limit: int = 500):
    """Get daily OHLCV for an equity symbol from Silver layer."""
    try:
        con = get_duckdb()
        rows = con.execute("""
            SELECT symbol, trade_date, open, high, low, close,
                   volume, vwap, trade_count
            FROM agora.main.silver_equity_ohlcv_daily
            WHERE symbol = ?
            ORDER BY trade_date DESC
            LIMIT ?
        """, [symbol.upper(), limit]).fetchall()
        con.close()
        cols = ["symbol", "trade_date", "open", "high", "low",
                "close", "volume", "vwap", "trade_count"]
        data = [dict(zip(cols, r)) for r in rows]
        for d in data:
            if d["trade_date"]:
                d["trade_date"] = str(d["trade_date"])
        return {"symbol": symbol.upper(), "data": data, "source": "duckdb_silver"}
    except Exception as e:
        log.error(f"DuckDB error: {e}")
        return {"symbol": symbol, "data": [], "error": str(e)}


@router.get("/symbols")
def get_symbols():
    """Get list of all available equity symbols."""
    cached = ttl_cache_get("symbols")
    if cached is not None:
        return cached
    try:
        con = get_duckdb()
        rows = con.execute("""
            SELECT DISTINCT symbol
            FROM agora.main.silver_equity_ohlcv_daily
            ORDER BY symbol
        """).fetchall()
        con.close()
        result = {"symbols": [r[0] for r in rows]}
        ttl_cache_set("symbols", result, ttl_seconds=3600)
        return result
    except Exception as e:
        log.error(f"DuckDB error: {e}")
        return {"symbols": [], "error": str(e)}

@router.get("/heatmap")
def get_heatmap():
    """Get latest price and 1d change % for all equity symbols in one query."""
    cached = ttl_cache_get("heatmap")
    if cached is not None:
        return cached
    try:
        con = get_duckdb()
        rows = con.execute("""
            WITH ranked AS (
                SELECT symbol, close, trade_date,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) as rn
                FROM agora.main.silver_equity_ohlcv_daily
            ),
            latest AS (SELECT symbol, close as c1, trade_date FROM ranked WHERE rn = 1),
            prev   AS (SELECT symbol, close as c2 FROM ranked WHERE rn = 2)
            SELECT l.symbol,
                   ROUND((l.c1 - p.c2) / NULLIF(p.c2, 0) * 100, 2) as change_pct
            FROM latest l
            LEFT JOIN prev p ON l.symbol = p.symbol
            ORDER BY l.symbol
        """).fetchall()
        con.close()
        result = {"data": {r[0]: r[1] for r in rows}, "source": "duckdb_silver"}
        ttl_cache_set("heatmap", result, ttl_seconds=300)
        return result
    except Exception as e:
        log.error(f"Heatmap error: {e}")
        return {"data": {}, "error": str(e)}

# Major indices mapping — Polygon tickers
INDICES = {
    "I:SPX":  {"sym": "S&P 500",    "pos": True},
    "I:NDX":  {"sym": "NASDAQ",     "pos": True},
    "I:DJI":  {"sym": "DOW JONES",  "pos": True},
    "I:RUT":  {"sym": "RUSSELL 2K", "pos": True},
    "I:VIX":  {"sym": "VIX",        "pos": False},
}

@router.get("/indices")
def get_indices():
    """Get major indices via Yahoo Finance HTTP API."""
    import time

    tickers = {
        "%5EGSPC": "S&P 500",
        "%5EIXIC":  "NASDAQ",
        "%5EDJI":   "DOW JONES",
        "%5ERUT":   "RUSSELL 2K",
        "%5EVIX":   "VIX",
        "%5EN225":  "NIKKEI 225",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://finance.yahoo.com/",
    }

    results = []
    for symbol, name in tickers.items():
        for attempt in range(3):
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
                resp = requests.get(url, headers=headers, timeout=15)
                data = resp.json()
                meta = data["chart"]["result"][0]["meta"]
                val  = float(meta["regularMarketPrice"])
                prev = float(meta.get("chartPreviousClose") or meta.get("previousClose") or val)
                chg  = val - prev
                pct  = (chg / prev) * 100 if prev else 0
                results.append({
                    "sym": name,
                    "val": round(val, 2),
                    "chg": round(chg, 2),
                    "pct": round(pct, 2),
                    "pos": chg >= 0,
                })
                time.sleep(0.5)
                break
            except Exception as e:
                log.error(f"Yahoo error for {symbol} attempt {attempt+1}: {e}")
                time.sleep(2)
                continue

    return {"data": results, "source": "yahoo_finance"}