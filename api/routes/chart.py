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
from datetime import datetime
import calendar

log = logging.getLogger("api.chart")
router = APIRouter()

QUESTDB_HOST = os.getenv("QUESTDB_HOST", "questdb")
QUESTDB_PORT = int(os.getenv("QUESTDB_PG_PORT", "8812"))
DUCKDB_PATH  = os.getenv("DUCKDB_PATH", "/app/transform/dbt/agora.duckdb")

CRYPTO_SYMBOLS = {"BTC", "ETH", "SOL"}

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

@router.get("/ohlcv/{symbol}")
def get_chart_ohlcv(symbol: str, timeframe: str = "1D", limit: int = 500):
    """
    Get OHLCV data in TradingView format.
    Crypto: from QuestDB (1m candles)
    Equities: from DuckDB (daily candles)
    """
    sym = symbol.upper().replace("-USD", "").replace("USDT", "")

    if sym in CRYPTO_SYMBOLS:
        return get_crypto_ohlcv(sym + "USDT", limit)
    else:
        return get_equity_ohlcv(sym, limit)

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

def get_equity_ohlcv(symbol: str, limit: int):
    """Fetch equity OHLCV from DuckDB in TradingView format."""
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        rows = conn.execute("""
            SELECT open, high, low, close, volume, trade_date
            FROM agora.main.silver_equity_ohlcv_daily
            WHERE symbol = ?
            ORDER BY trade_date ASC
            LIMIT ?
        """, [symbol, limit]).fetchall()
        conn.close()
        data = []
        for r in rows:
            ts = to_unix(str(r[5])[:10])
            if ts:
                data.append({
                    "time":   ts,
                    "open":   float(r[0]),
                    "high":   float(r[1]),
                    "low":    float(r[2]),
                    "close":  float(r[3]),
                    "volume": float(r[4]),
                })
        return {"symbol": symbol, "timeframe": "1D", "data": data, "source": "duckdb"}
    except Exception as e:
        log.error(f"DuckDB chart error: {e}")
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
        rows = conn.execute("""
            SELECT DISTINCT symbol
            FROM agora.main.silver_equity_ohlcv_daily
            ORDER BY symbol
        """).fetchall()
        conn.close()
        equities = [{"symbol": r[0], "name": r[0], "type": "equity"} for r in rows]
    except Exception as e:
        log.error(f"Symbols fetch error: {e}")
        equities = []
    return {"symbols": crypto + equities}