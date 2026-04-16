"""
Macro routes - serves FRED economic data and live market data
Writes all fetched data to QuestDB for historical storage
"""
import os
import logging
import time
import threading
from datetime import datetime, timezone
from fastapi import APIRouter
import requests
import psycopg2
import psycopg2.extras

log = logging.getLogger("api.macro")

# Simple TTL cache
_cache = {}
_cache_lock = threading.Lock()

def ttl_cache_get(key: str):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() < entry["expires"]:
            return entry["value"]
    return None

def ttl_cache_set(key: str, value, ttl_seconds: int):
    with _cache_lock:
        _cache[key] = {"value": value, "expires": time.time() + ttl_seconds}
router = APIRouter()

FRED_API_KEY = os.getenv("FRED_API_KEY", "")
FRED_BASE    = "https://api.stlouisfed.org/fred/series/observations"

QUESTDB_HOST = os.getenv("QUESTDB_HOST", "questdb")
QUESTDB_PORT = int(os.getenv("QUESTDB_PG_PORT", "8812"))

MACRO_SERIES = {
    "fed_rate":       "FEDFUNDS",
    "treasury_10y":   "GS10",
    "cpi":            "CPIAUCSL",
    "unemployment":   "UNRATE",
    "gdp_growth":     "A191RL1Q225SBEA",
    "vix":            "VIXCLS",
    "dxy":            "DTWEXBGS",
}

FOREX_TICKERS = {
    "EURUSD": "EURUSD=X",
    "GBPUSD": "GBPUSD=X",
    "USDJPY": "JPY=X",
    "USDCHF": "CHF=X",
    "AUDUSD": "AUDUSD=X",
    "USDCAD": "CAD=X",
    "USDCNY": "CNY=X",
    "USDINR": "INR=X",
}

COMMODITIES_TICKERS = {
    "GOLD":   "GC=F",
    "OIL":    "CL=F",
    "SILVER": "SI=F",
    "NATGAS": "NG=F",
    "COPPER": "HG=F",
    "WHEAT":  "ZW=F",
}

INDEX_TICKERS = {
    "SPX":    "%5EGSPC",
    "NASDAQ": "%5EIXIC",
    "DOW":    "%5EDJI",
    "RUT":    "%5ERUT",
    "VIX":    "%5EVIX",
    "NKY":    "%5EN225",
}

YF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

def get_questdb():
    return psycopg2.connect(
        host=QUESTDB_HOST,
        port=QUESTDB_PORT,
        user="admin",
        password="quest",
        database="qdb",
    )

def write_to_questdb(table: str, rows: list):
    """Write a list of dicts to a QuestDB table."""
    if not rows:
        return
    try:
        conn = get_questdb()
        cur = conn.cursor()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000Z")
        for row in rows:
            if table == "forex_rates":
                cur.execute(
                    "INSERT INTO forex_rates (symbol, price, change_pct, timestamp) VALUES (%s, %s, %s, %s)",
                    (row["symbol"], row["price"], row["change_pct"], now)
                )
            elif table == "commodity_prices":
                cur.execute(
                    "INSERT INTO commodity_prices (symbol, price, change_pct, timestamp) VALUES (%s, %s, %s, %s)",
                    (row["symbol"], row["price"], row["change_pct"], now)
                )
            elif table == "index_prices":
                cur.execute(
                    "INSERT INTO index_prices (symbol, price, change, change_pct, timestamp) VALUES (%s, %s, %s, %s, %s)",
                    (row["symbol"], row["price"], row["change"], row["change_pct"], now)
                )
        conn.commit()
        conn.close()
        log.info(f"Wrote {len(rows)} rows to {table}")
    except Exception as e:
        log.error(f"QuestDB write error for {table}: {e}")

def get_change_pct_from_history(table: str, symbols: list) -> dict:
    """Calculate 24h change % from QuestDB historical data."""
    try:
        conn = get_questdb()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT symbol, price FROM " + table + " WHERE timestamp <= dateadd('h', -24, now()) LATEST ON timestamp PARTITION BY symbol"
        )
        prev_prices = {r["symbol"]: r["price"] for r in cur.fetchall()}
        cur.execute(
            "SELECT symbol, price FROM " + table + " LATEST ON timestamp PARTITION BY symbol"
        )
        curr_prices = {r["symbol"]: r["price"] for r in cur.fetchall()}
        conn.close()
        result = {}
        for sym in symbols:
            curr = curr_prices.get(sym)
            prev = prev_prices.get(sym)
            if curr and prev and prev != 0:
                result[sym] = round((curr - prev) / prev * 100, 4)
            else:
                result[sym] = 0.0
        return result
    except Exception as e:
        log.error(f"History change_pct error for {table}: {e}")
        return {sym: 0.0 for sym in symbols}

def fetch_fred(series_id: str, limit: int = 1) -> dict:
    """Fetch latest observation from FRED."""
    try:
        resp = requests.get(FRED_BASE, params={
            "series_id":  series_id,
            "api_key":    FRED_API_KEY,
            "file_type":  "json",
            "sort_order": "desc",
            "limit":      limit,
        }, timeout=10)
        data = resp.json()
        obs = data.get("observations", [])
        if obs:
            return {"value": obs[0]["value"], "date": obs[0]["date"]}
        return {"value": None, "date": None}
    except Exception as e:
        log.error(f"FRED error for {series_id}: {e}")
        return {"value": None, "date": None, "error": str(e)}

def fetch_yf_quote(ticker: str) -> dict:
    """Fetch a single Yahoo Finance quote with retries."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "1d", "range": "2d"}
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=YF_HEADERS, params=params, timeout=10)
            data = resp.json()
            meta = data["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice") or meta.get("previousClose")
            prev  = meta.get("previousClose", price)
            change = round(price - prev, 4) if prev else 0
            change_pct = round(((price - prev) / prev) * 100, 4) if prev else 0
            return {
                "price":      round(price, 4),
                "prev_close": round(prev, 4),
                "change":     change,
                "change_pct": change_pct,
                "currency":   meta.get("currency", "USD"),
            }
        except Exception as e:
            log.warning(f"YF attempt {attempt+1} failed for {ticker}: {e}")
            time.sleep(0.5)
    return {"price": None, "prev_close": None, "change": None, "change_pct": None, "error": "fetch_failed"}

@router.get("/pulse")
def get_macro_pulse():
    """Get latest macro indicators from FRED."""
    if not FRED_API_KEY:
        return {
            "data": {
                "fed_rate":     {"label": "Fed Funds Rate", "value": "5.25-5.50%", "date": "2024-01"},
                "treasury_10y": {"label": "10Y Treasury",   "value": "4.31%",      "date": "2024-03"},
                "cpi":          {"label": "CPI Inflation",  "value": "3.1%",       "date": "2024-02"},
                "unemployment": {"label": "Unemployment",   "value": "3.7%",       "date": "2024-02"},
                "vix":          {"label": "VIX",            "value": "18.42",      "date": "2024-03"},
            },
            "source": "mock_no_fred_key"
        }
    cached = ttl_cache_get("macro_pulse")
    if cached is not None:
        return cached
    result = {}
    labels = {
        "fed_rate":     "Fed Funds Rate",
        "treasury_10y": "10Y Treasury",
        "cpi":          "CPI Inflation",
        "unemployment": "Unemployment",
        "gdp_growth":   "GDP Growth",
        "vix":          "VIX",
        "dxy":          "USD Index (DXY)",
    }
    for key, series_id in MACRO_SERIES.items():
        obs = fetch_fred(series_id)
        result[key] = {"label": labels[key], **obs}
    out = {"data": result, "source": "fred"}
    ttl_cache_set("macro_pulse", out, ttl_seconds=600)
    return out

@router.get("/forex")
def get_forex():
    """Live forex rates via Yahoo Finance - writes to QuestDB, calculates change from history."""
    cached = ttl_cache_get("forex")
    if cached is not None:
        return cached
    result = {}
    rows_to_store = []
    for pair, ticker in FOREX_TICKERS.items():
        quote = fetch_yf_quote(ticker)
        result[pair] = quote
        if quote.get("price"):
            rows_to_store.append({
                "symbol":     pair,
                "price":      quote["price"],
                "change_pct": quote["change_pct"] or 0,
            })
        time.sleep(0.5)
    write_to_questdb("forex_rates", rows_to_store)
    change_pcts = get_change_pct_from_history("forex_rates", list(FOREX_TICKERS.keys()))
    for pair in result:
        if result[pair].get("price"):
            result[pair]["change_pct"] = change_pcts.get(pair, 0.0)
    out = {"data": result, "source": "yahoo_finance"}
    ttl_cache_set("forex", out, ttl_seconds=120)
    return out

@router.get("/commodities")
def get_commodities():
    """Live commodity prices via Yahoo Finance - writes to QuestDB, calculates change from history."""
    cached = ttl_cache_get("commodities")
    if cached is not None:
        return cached
    result = {}
    rows_to_store = []
    for commodity, ticker in COMMODITIES_TICKERS.items():
        quote = fetch_yf_quote(ticker)
        result[commodity] = quote
        if quote.get("price"):
            rows_to_store.append({
                "symbol":     commodity,
                "price":      quote["price"],
                "change_pct": quote["change_pct"] or 0,
            })
        time.sleep(0.5)
    write_to_questdb("commodity_prices", rows_to_store)
    change_pcts = get_change_pct_from_history("commodity_prices", list(COMMODITIES_TICKERS.keys()))
    for commodity in result:
        if result[commodity].get("price"):
            result[commodity]["change_pct"] = change_pcts.get(commodity, 0.0)
    out = {"data": result, "source": "yahoo_finance"}
    ttl_cache_set("commodities", out, ttl_seconds=120)
    return out

@router.get("/indices")
def get_indices():
    """Live index prices via Yahoo Finance - writes to QuestDB."""
    result = {}
    rows_to_store = []
    for symbol, ticker in INDEX_TICKERS.items():
        quote = fetch_yf_quote(ticker)
        result[symbol] = quote
        if quote.get("price"):
            rows_to_store.append({
                "symbol":     symbol,
                "price":      quote["price"],
                "change":     quote["change"] or 0,
                "change_pct": quote["change_pct"] or 0,
            })
        time.sleep(0.5)
    write_to_questdb("index_prices", rows_to_store)
    return {"data": result, "source": "yahoo_finance"}

@router.get("/fear-greed")
def get_fear_greed():
    """Live Fear & Greed Index from CNN."""
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://edition.cnn.com/markets/fear-and-greed",
    }
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            data = resp.json()
            fg = data.get("fear_and_greed", {})
            return {
                "score": round(fg.get("score", 0), 1),
                "rating": fg.get("rating", "unknown").replace("_", " ").title(),
                "previous_close": round(fg.get("previous_close", 0), 1),
                "previous_1_week": round(fg.get("previous_1_week", 0), 1),
                "previous_1_month": round(fg.get("previous_1_month", 0), 1),
                "source": "cnn",
            }
        except Exception as e:
            log.warning(f"Fear & Greed attempt {attempt+1} failed: {e}")
            time.sleep(0.5)
    return {"score": None, "rating": "unavailable", "source": "cnn", "error": "fetch_failed"}

CRYPTO_TICKERS = {
    "BTC-USD": "BTC-USD",
    "ETH-USD": "ETH-USD",
    "SOL-USD": "SOL-USD",
}

@router.get("/crypto-change")
def get_crypto_change(ticker: str = "BTC-USD"):
    """Get 24h change % for a crypto via Yahoo Finance."""
    result = fetch_yf_quote(ticker)
    return {
        "ticker": ticker,
        "price": result.get("price"),
        "change_pct": result.get("change_pct"),
        "source": "yahoo_finance",
    }