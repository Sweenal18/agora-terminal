"""
Macro routes — serves FRED economic data
"""
import os
import logging
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


def fetch_fred(series_id: str, limit: int = 1) -> dict:
    """Fetch latest observation from FRED."""
    try:
        resp = requests.get(FRED_BASE, params={
            "series_id":      series_id,
            "api_key":        FRED_API_KEY,
            "file_type":      "json",
            "sort_order":     "desc",
            "limit":          limit,
        }, timeout=10)
        data = resp.json()
        obs = data.get("observations", [])
        if obs:
            return {"value": obs[0]["value"], "date": obs[0]["date"]}
        return {"value": None, "date": None}
    except Exception as e:
        log.error(f"FRED error for {series_id}: {e}")
        return {"value": None, "date": None, "error": str(e)}


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
