"""
Chart routes - serves OHLCV data in TradingView Lightweight Charts format
Supports both crypto (QuestDB) and equities (DuckDB)
"""
import os
import logging
from fastapi import APIRouter, HTTPException
import psycopg2
import psycopg2.extras
import duckdb
from datetime import datetime, timedelta
import calendar
from threading import Lock
import time

# Simple TTL cache
_cache = {}
_cache_lock = Lock()

def ttl_cache_get(key: str):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() < entry["expires"]:
            return entry["value"]
    return None

def ttl_cache_set(key: str, value, ttl_seconds: int):
    with _cache_lock:
        _cache[key] = {"value": value, "expires": time.time() + ttl_seconds}

log = logging.getLogger("api.chart")
router = APIRouter()

QUESTDB_HOST = os.getenv("QUESTDB_HOST", "questdb")
QUESTDB_PORT = int(os.getenv("QUESTDB_PG_PORT", "8812"))
DUCKDB_PATH  = os.getenv("DUCKDB_PATH", "/app/transform/dbt/agora.duckdb")

CRYPTO_SYMBOLS = {"BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE"}

FOREX_SYMBOLS = {"EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD"}
COMMODITY_SYMBOLS = {"GOLD", "OIL", "SILVER", "NATGAS"}
COMMODITY_YAHOO_MAP = {
    "GOLD": "GC=F", "OIL": "CL=F", "SILVER": "SI=F", "NATGAS": "NG=F"
}

TIMEFRAME_DAYS = {
    "1W":  7,
    "1M":  30,
    "3M":  90,
    "6M":  180,
    "1Y":  365,
    "2Y":  730,
    "5Y":  1825,
    "MAX": None,
}

def get_questdb():
    return psycopg2.connect(
        host=QUESTDB_HOST,
        port=QUESTDB_PORT,
        user="admin",
        password="quest",
        database="qdb",
    )

def to_unix(ts_str: str) -> int:
    """Convert timestamp string to Unix seconds."""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(str(ts_str)[:19], fmt)
            return calendar.timegm(dt.timetuple())
        except Exception:
            continue
    return 0

def timeframe_to_start_date(timeframe: str) -> str | None:
    """Convert timeframe string to ISO start date, or None for MAX."""
    days = TIMEFRAME_DAYS.get(timeframe.upper())
    if days is None:
        return None
    return (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

@router.get("/ohlcv/{symbol}")
def get_chart_ohlcv(symbol: str, timeframe: str = "1Y", limit: int = 1000):
    """
    Get OHLCV data in TradingView format.
    Crypto: from QuestDB (1m candles)
    Equities: from DuckDB (daily candles)
    Timeframes: 1W, 1M, 3M, 6M, 1Y, 2Y, 5Y, MAX
    """
    sym = symbol.upper().replace("-USD", "").replace("USDT", "")

    if sym in CRYPTO_SYMBOLS:
        return get_crypto_ohlcv(sym + "USDT", limit)
    elif sym in FOREX_SYMBOLS:
        return get_yahoo_ohlcv(sym + "=X", sym, timeframe, limit)
    elif sym in COMMODITY_SYMBOLS:
        yahoo_sym = COMMODITY_YAHOO_MAP.get(sym, sym)
        return get_yahoo_ohlcv(yahoo_sym, sym, timeframe, limit)
    else:
        result = get_equity_ohlcv(sym, timeframe, limit)
        if not result["data"]:
            return get_yahoo_ohlcv(sym, sym, timeframe, limit)
        return result

def get_yahoo_ohlcv(yahoo_symbol: str, display_symbol: str, timeframe: str, limit: int):
    """Fetch OHLCV from Yahoo Finance for forex/commodities/unknown symbols."""
    import requests
    import time

    # Map timeframe to Yahoo range param
    yf_range_map = {
        "1W": "5d", "1M": "1mo", "3M": "3mo", "6M": "6mo",
        "1Y": "1y", "2Y": "2y", "5Y": "5y", "MAX": "10y",
    }
    yf_range = yf_range_map.get(timeframe.upper(), "1y")

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    for attempt in range(3):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
            params = {"interval": "1d", "range": yf_range}
            res = requests.get(url, params=params, headers=headers, timeout=15)
            res.raise_for_status()
            d = res.json()
            result = d["chart"]["result"][0]
            timestamps = result["timestamp"]
            ohlcv = result["indicators"]["quote"][0]
            adjclose = result["indicators"].get("adjclose", [{}])[0].get("adjclose", [])
            data = []
            for i, ts in enumerate(timestamps):
                try:
                    o = ohlcv["open"][i]
                    h = ohlcv["high"][i]
                    low = ohlcv["low"][i]
                    c = (adjclose[i] if adjclose and i < len(adjclose) and adjclose[i] else ohlcv["close"][i])
                    v = ohlcv["volume"][i] or 0
                    if None in (o, h, low, c):
                        continue
                    data.append({
                        "time":   ts,
                        "open":   round(float(o), 4),
                        "high":   round(float(h), 4),
                        "low":    round(float(low), 4),
                        "close":  round(float(c), 4),
                        "volume": float(v),
                    })
                except Exception:
                    continue
            return {"symbol": display_symbol, "timeframe": timeframe, "data": data[-limit:], "source": "yahoo"}
        except Exception as e:
            log.warning(f"Yahoo chart attempt {attempt+1} for {yahoo_symbol}: {e}")
            time.sleep(0.5)
    return {"symbol": display_symbol, "timeframe": timeframe, "data": [], "source": "yahoo"}

def get_crypto_ohlcv(symbol: str, limit: int):
    """Fetch crypto OHLCV from QuestDB in TradingView format."""
    try:
        conn = get_questdb()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT open, high, low, close, volume, timestamp
            FROM ohlcv_1m
            WHERE symbol = %s
            ORDER BY timestamp ASC
            LIMIT %s
        """, (symbol, limit))
        rows = cur.fetchall()
        conn.close()
        data = []
        for r in rows:
            ts = to_unix(r["timestamp"])
            if ts:
                data.append({
                    "time":   ts,
                    "open":   float(r["open"]),
                    "high":   float(r["high"]),
                    "low":    float(r["low"]),
                    "close":  float(r["close"]),
                    "volume": float(r["volume"]),
                })
        return {"symbol": symbol, "timeframe": "1m", "data": data, "source": "questdb"}
    except Exception as e:
        log.error(f"QuestDB chart error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def get_equity_ohlcv(symbol: str, timeframe: str, limit: int):
    """Fetch equity OHLCV from DuckDB with timeframe filtering."""
    cache_key = f"ohlcv:{symbol}:{timeframe}:{limit}"
    cached = ttl_cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        start_date = timeframe_to_start_date(timeframe)
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        if start_date:
            rows = conn.execute("""
                SELECT trade_date, MAX(open) as open, MAX(high) as high, MIN(low) as low, MAX(close) as close, MAX(volume) as volume
                FROM agora.main.silver_equity_ohlcv_daily
                WHERE symbol = ? AND trade_date >= CAST(? AS DATE)
                GROUP BY trade_date
                ORDER BY trade_date ASC
                LIMIT ?
            """, [symbol, start_date, limit]).fetchall()
        else:
            rows = conn.execute("""
                SELECT trade_date, MAX(open) as open, MAX(high) as high, MIN(low) as low, MAX(close) as close, MAX(volume) as volume
                FROM agora.main.silver_equity_ohlcv_daily
                WHERE symbol = ?
                GROUP BY trade_date
                ORDER BY trade_date ASC
                LIMIT ?
            """, [symbol, limit]).fetchall()
        conn.close()
        data = []
        for r in rows:
            ts = to_unix(str(r[0])[:10])
            if ts:
                data.append({
                    "time":   ts,
                    "open":   float(r[1]),
                    "high":   float(r[2]),
                    "low":    float(r[3]),
                    "close":  float(r[4]),
                    "volume": float(r[5]),
                })
        result = {"symbol": symbol, "timeframe": timeframe, "data": data, "source": "duckdb"}
        ttl_cache_set(cache_key, result, ttl_seconds=900)
        return result
    except Exception as e:
        log.error(f"DuckDB chart error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/info/{symbol}")
def get_chart_info(symbol: str):
    """
    Get symbol info for chart sidebar.
    Returns company name, sector, market cap, 52w high/low, beta, P/B.
    """
    sym = symbol.upper()

    # Crypto info
    crypto_info = {
        "BTC": {"name": "Bitcoin",  "sector": "Crypto", "asset_class": "crypto"},
        "ETH": {"name": "Ethereum", "sector": "Crypto", "asset_class": "crypto"},
        "SOL": {"name": "Solana",   "sector": "Crypto", "asset_class": "crypto"},
    }
    if sym in crypto_info:
        return {"symbol": sym, **crypto_info[sym], "source": "static"}

    # Equity info from DuckDB Gold
    cache_key = f"info:{sym}"
    cached = ttl_cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)

        # Get instrument master from dim_instruments
        inst = conn.execute("""
            SELECT company_name, sector, industry, asset_class, market_cap, market_cap_bucket, currency
            FROM agora.main_gold.dim_instruments
            WHERE symbol = ?
            LIMIT 1
        """, [sym]).fetchone()

        # Get latest fundamentals
        fund = conn.execute("""
            SELECT beta, week_52_high, week_52_low, price_to_book,
                   roe, dividend_yield, market_cap
            FROM agora.main_gold.fct_fundamentals
            WHERE symbol = ?
            ORDER BY snapshot_date DESC
            LIMIT 1
        """, [sym]).fetchone()

        # Get latest price + daily change
        price_row = conn.execute("""
            SELECT close, daily_return_pct, trade_date
            FROM agora.main_gold.fct_prices
            WHERE symbol = ?
            ORDER BY trade_date DESC
            LIMIT 1
        """, [sym]).fetchone()

        conn.close()

        result = {"symbol": sym, "source": "duckdb"}

        if inst:
            result["name"]             = inst[0]
            result["sector"]           = inst[1]
            result["industry"]         = inst[2]
            result["asset_class"]      = inst[3]
            result["market_cap"]       = inst[4]
            result["market_cap_bucket"] = inst[5]
            result["currency"]         = inst[6]

        if fund:
            result["beta"]            = fund[0]
            result["week_52_high"]    = fund[1]
            result["week_52_low"]     = fund[2]
            result["price_to_book"]   = round(fund[3], 2) if fund[3] else None
            result["roe"]             = round(fund[4], 4) if fund[4] else None
            result["dividend_yield"]  = round(fund[5], 4) if fund[5] else None

        if price_row:
            result["last_close"]      = price_row[0]
            result["daily_return_pct"] = round(price_row[1], 4) if price_row[1] else None
            result["last_trade_date"] = str(price_row[2])

        ttl_cache_set(cache_key, result, ttl_seconds=300)
        return result

    except Exception as e:
        log.error(f"Chart info error for {sym}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/symbols")
def get_chart_symbols():
    """Get all available symbols for the chart terminal."""
    crypto = [
        {"symbol": "BTC", "name": "Bitcoin",  "type": "crypto"},
        {"symbol": "ETH", "name": "Ethereum", "type": "crypto"},
        {"symbol": "SOL", "name": "Solana",   "type": "crypto"},
    ]
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        # Join with dim_instruments to get company names
        rows = conn.execute("""
            SELECT s.symbol, COALESCE(d.company_name, s.symbol) as name
            FROM (SELECT DISTINCT symbol FROM agora.main.silver_equity_ohlcv_daily) s
            LEFT JOIN agora.main_gold.dim_instruments d ON s.symbol = d.symbol AND d.is_current = TRUE
            ORDER BY s.symbol
        """).fetchall()
        conn.close()
        equities = [{"symbol": r[0], "name": r[1], "type": "equity"} for r in rows]
    except Exception as e:
        log.error(f"Symbols fetch error: {e}")
        equities = []
    return {"symbols": crypto + equities}