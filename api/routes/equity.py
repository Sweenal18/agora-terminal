"""
Equity routes — serves S&P 500 OHLCV data from DuckDB Silver layer
"""
import os
import logging
from fastapi import APIRouter
import duckdb

log = logging.getLogger("api.equity")
router = APIRouter()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/transform/dbt/agora.duckdb")


def get_duckdb():
    return duckdb.connect(DUCKDB_PATH, read_only=True)


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
    try:
        con = get_duckdb()
        rows = con.execute("""
            SELECT DISTINCT symbol
            FROM agora.main.silver_equity_ohlcv_daily
            ORDER BY symbol
        """).fetchall()
        con.close()
        return {"symbols": [r[0] for r in rows]}
    except Exception as e:
        log.error(f"DuckDB error: {e}")
        return {"symbols": [], "error": str(e)}



