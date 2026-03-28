"""
Fetch ETF OHLCV data (SPY, QQQ, DIA, IWM) via Yahoo Finance
and append to polygon_bronze.jsonl in the same schema
"""
import json
import time
import requests
from datetime import datetime, timezone

TICKERS = ["SPY", "QQQ", "DIA", "IWM"]
OUTPUT_FILE = "polygon_bronze.jsonl"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def fetch_yahoo_ohlcv(ticker):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "1d", "range": "2y"}
    try:
        res = requests.get(url, params=params, headers=HEADERS, timeout=15)
        res.raise_for_status()
        data = res.json()
        result = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        ohlcv = result["indicators"]["quote"][0]
        adjclose = result["indicators"]["adjclose"][0]["adjclose"]
        bars = []
        for i, ts in enumerate(timestamps):
            if not all([ohlcv["open"][i], ohlcv["high"][i], ohlcv["low"][i], ohlcv["close"][i]]):
                continue
            bars.append({
                "symbol": ticker,
                "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
                "open": round(ohlcv["open"][i], 4),
                "high": round(ohlcv["high"][i], 4),
                "low": round(ohlcv["low"][i], 4),
                "close": round(adjclose[i] or ohlcv["close"][i], 4),
                "volume": int(ohlcv["volume"][i] or 0),
                "vwap": None,
                "trade_count": None,
                "timestamp_ms": ts * 1000,
                "ingested_at_ms": int(datetime.now(timezone.utc).timestamp() * 1000),
                "source": "yahoo",
                "adjusted": True,
            })
        print(f"{ticker}: {len(bars)} bars fetched")
        return bars
    except Exception as e:
        print(f"{ticker}: ERROR - {e}")
        return []

with open(OUTPUT_FILE, "a") as f:
    for ticker in TICKERS:
        bars = fetch_yahoo_ohlcv(ticker)
        for bar in bars:
            f.write(json.dumps(bar) + "\n")
        time.sleep(1)

print("Done. ETF data appended to polygon_bronze.jsonl")