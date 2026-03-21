"""
Crypto routes — serves live prices and OHLCV candles from QuestDB
"""
import os
import logging
from fastapi import APIRouter
import psycopg2
import psycopg2.extras

log = logging.getLogger("api.crypto")
router = APIRouter()

QUESTDB_HOST = os.getenv("QUESTDB_HOST", "questdb")
QUESTDB_PORT = int(os.getenv("QUESTDB_PG_PORT", "8812"))
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASS = os.getenv("QUESTDB_PASS", "quest")


def get_questdb():
    return psycopg2.connect(
        host=QUESTDB_HOST,
        port=QUESTDB_PORT,
        user=QUESTDB_USER,
        password=QUESTDB_PASS,
        database="qdb",
    )


@router.get("/prices")
def get_latest_prices():
    """Get latest price for BTC, ETH, SOL from QuestDB."""
    try:
        conn = get_questdb()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT symbol, close as price, volume, trade_count,
                   open, high, low, timestamp
            FROM ohlcv_1m
            LATEST ON timestamp PARTITION BY symbol
        """)
        rows = cur.fetchall()
        conn.close()
        data = []
        for r in rows:
            row = dict(r)
            if row.get("timestamp"):
                row["timestamp"] = str(row["timestamp"])
            data.append(row)
        return {"data": data, "source": "questdb"}
    except Exception as e:
        log.error(f"QuestDB error: {e}")
        return {"data": [], "error": str(e)}


@router.get("/ohlcv/{symbol}")
def get_ohlcv(symbol: str, limit: int = 60):
    """Get last N 1-minute candles for a symbol."""
    try:
        conn = get_questdb()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT symbol, open, high, low, close, volume,
                   trade_count, timestamp
            FROM ohlcv_1m
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (symbol.upper(), limit))
        rows = cur.fetchall()
        conn.close()
        data = []
        for r in rows:
            row = dict(r)
            if row.get("timestamp"):
                row["timestamp"] = str(row["timestamp"])
            data.append(row)
        return {"symbol": symbol.upper(), "data": data, "source": "questdb"}
    except Exception as e:
        log.error(f"QuestDB error: {e}")
        return {"symbol": symbol, "data": [], "error": str(e)}
