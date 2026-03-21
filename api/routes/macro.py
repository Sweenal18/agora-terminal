"""
Macro routes - serves FRED economic data and live market data
"""
import os
import logging
import time
from fastapi import APIRouter
import requests

log = logging.getLogger("api.macro")
router = APIRouter()

FRED_API_KEY = os.getenv("FRED_API_KEY", "")
FRED_BASE    = "https://api.stlouisfed.org/fred/series/observations"

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

YF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

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
    params = {"interval": "1d", "range": "1d"}
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=YF_HEADERS, params=params, timeout=10)
            data = resp.json()
            meta = data["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice") or meta.get("previousClose")
            prev  = meta.get("previousClose", price)
            change_pct = round(((price - prev) / prev) * 100, 4) if prev else 0
            return {
                "price":      round(price, 4),
                "prev_close": round(prev, 4),
                "change_pct": change_pct,
                "currency":   meta.get("currency", "USD"),
            }
        except Exception as e:
            log.warning(f"YF attempt {attempt+1} failed for {ticker}: {e}")
            time.sleep(0.5)
    return {"price": None, "prev_close": None, "change_pct": None, "error": "fetch_failed"}

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
    return {"data": result, "source": "fred"}

@router.get("/forex")
def get_forex():
    """Live forex rates via Yahoo Finance."""
    result = {}
    for pair, ticker in FOREX_TICKERS.items():
        result[pair] = fetch_yf_quote(ticker)
        time.sleep(0.5)
    return {"data": result, "source": "yahoo_finance"}

@router.get("/commodities")
def get_commodities():
    """Live commodity prices via Yahoo Finance."""
    result = {}
    for commodity, ticker in COMMODITIES_TICKERS.items():
        result[commodity] = fetch_yf_quote(ticker)
        time.sleep(0.5)
    return {"data": result, "source": "yahoo_finance"}