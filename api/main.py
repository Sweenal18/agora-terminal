"""
Agora Terminal -- FastAPI Backend
Serves real-time financial data to the dashboard
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import crypto, equity, macro, chart, screener, auth, filings
from api.ai_query import router as ai_query_router

app = FastAPI(
    title="Agora Terminal API",
    description="Real-time financial intelligence API",
    version="0.1.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(crypto.router,   prefix="/api/crypto",   tags=["crypto"])
app.include_router(equity.router,   prefix="/api/equity",   tags=["equity"])
app.include_router(macro.router,    prefix="/api/macro",    tags=["macro"])
app.include_router(chart.router,    prefix="/api/chart",    tags=["chart"])
app.include_router(screener.router, prefix="/api/screener", tags=["screener"])
app.include_router(auth.router)
app.include_router(ai_query_router.router)
app.include_router(filings.router, prefix="/api", tags=["filings"])

@app.get("/health")
def health():
    return {"status": "ok", "service": "agora-terminal-api"}

@app.get("/api/health/data")
def data_freshness():
    """Show when each data source was last updated."""
    import duckdb
    import os
    from datetime import datetime, timezone
    DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/transform/dbt/agora.duckdb")
    result = {}
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        # Equity OHLCV freshness
        row = conn.execute("SELECT MAX(trade_date), COUNT(DISTINCT symbol) FROM agora.main.silver_equity_ohlcv_daily").fetchone()
        result["equity_ohlcv"] = {"last_date": str(row[0]), "symbols": row[1]}
        # Macro freshness
        row = conn.execute("SELECT MAX(date), COUNT(DISTINCT indicator) FROM agora.main.silver_macro_pulse").fetchone()
        result["macro"] = {"last_date": str(row[0]), "indicators": row[1]}
        # Fundamentals freshness
        row = conn.execute("SELECT MAX(updated_at), COUNT(DISTINCT symbol) FROM agora.main.silver_equity_fundamentals").fetchone()
        result["fundamentals"] = {"last_date": str(row[0]), "symbols": row[1]}
        # Gold fct_prices freshness
        row = conn.execute("SELECT MAX(trade_date), COUNT(DISTINCT symbol) FROM agora.main_gold.fct_prices").fetchone()
        result["fct_prices"] = {"last_date": str(row[0]), "symbols": row[1]}
        conn.close()
    except Exception as e:
        result["duckdb_error"] = str(e)
    result["checked_at"] = datetime.now(timezone.utc).isoformat()
    return result