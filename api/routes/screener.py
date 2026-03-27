"""
Asset Screener routes - combines fundamentals + technicals for filtering
"""
import os
import logging
import duckdb
from fastapi import APIRouter

log = logging.getLogger("api.screener")
router = APIRouter()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/transform/dbt/agora.duckdb")

@router.get("/screen")
def screen_assets(
    sector: str = None,
    min_market_cap: float = None,
    max_market_cap: float = None,
    min_beta: float = None,
    max_beta: float = None,
    min_roe: float = None,
    max_ev_ebitda: float = None,
    min_div_yield: float = None,
    min_pct_from_low: float = None,
    max_pct_from_high: float = None,
    sort_by: str = "market_cap",
    sort_dir: str = "desc",
    limit: int = 50,
):
    """Screen stocks by fundamental and technical criteria."""
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        query = """
            WITH latest_price AS (
                SELECT
                    symbol,
                    close as price,
                    volume,
                    trade_date,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) as rn
                FROM agora.main.silver_equity_ohlcv_daily
            ),
            price_changes AS (
                SELECT
                    symbol,
                    MAX(CASE WHEN rn = 1 THEN price END) as price,
                    MAX(CASE WHEN rn = 1 THEN volume END) as volume,
                    MAX(CASE WHEN rn = 1 THEN trade_date END) as last_date,
                    MAX(CASE WHEN rn = 1 THEN price END) - MAX(CASE WHEN rn = 2 THEN price END) as change_1d,
                    (MAX(CASE WHEN rn = 1 THEN price END) - MAX(CASE WHEN rn = 2 THEN price END))
                        / NULLIF(MAX(CASE WHEN rn = 2 THEN price END), 0) * 100 as change_1d_pct,
                    MAX(CASE WHEN rn = 1 THEN price END) - MAX(CASE WHEN rn = 5 THEN price END) as change_1w,
                    (MAX(CASE WHEN rn = 1 THEN price END) - MAX(CASE WHEN rn = 5 THEN price END))
                        / NULLIF(MAX(CASE WHEN rn = 5 THEN price END), 0) * 100 as change_1w_pct,
                    MAX(CASE WHEN rn = 1 THEN price END) - MAX(CASE WHEN rn = 21 THEN price END) as change_1m,
                    (MAX(CASE WHEN rn = 1 THEN price END) - MAX(CASE WHEN rn = 21 THEN price END))
                        / NULLIF(MAX(CASE WHEN rn = 21 THEN price END), 0) * 100 as change_1m_pct
                FROM latest_price
                WHERE rn <= 21
                GROUP BY symbol
            ),
            moving_avgs AS (
                SELECT
                    symbol,
                    AVG(CASE WHEN rn <= 20 THEN price END) as ma20,
                    AVG(CASE WHEN rn <= 50 THEN price END) as ma50,
                    AVG(CASE WHEN rn <= 200 THEN price END) as ma200
                FROM latest_price
                WHERE rn <= 200
                GROUP BY symbol
            )
            SELECT
                f.symbol,
                f.company_name,
                f.sector,
                f.industry,
                f.market_cap,
                f.beta,
                f.week_52_high,
                f.week_52_low,
                f.roe,
                f.ev_to_ebitda,
                f.price_to_book,
                f.price_to_sales,
                f.dividend_yield,
                f.roic,
                f.current_ratio,
                f.logo_url,
                p.price,
                p.volume,
                p.change_1d_pct,
                p.change_1w_pct,
                p.change_1m_pct,
                m.ma20,
                m.ma50,
                m.ma200,
                CASE WHEN f.week_52_low > 0
                    THEN (p.price - f.week_52_low) / f.week_52_low * 100
                    ELSE NULL END as pct_from_52w_low,
                CASE WHEN f.week_52_high > 0
                    THEN (p.price - f.week_52_high) / f.week_52_high * 100
                    ELSE NULL END as pct_from_52w_high,
                CASE WHEN m.ma50 > 0
                    THEN (p.price - m.ma50) / m.ma50 * 100
                    ELSE NULL END as pct_from_ma50
            FROM agora.main.silver_equity_fundamentals f
            LEFT JOIN price_changes p ON f.symbol = p.symbol
            LEFT JOIN moving_avgs m ON f.symbol = m.symbol
            WHERE 1=1
        """
        params = []
        if sector:
            query += " AND f.sector = ?"
            params.append(sector)
        if min_market_cap:
            query += " AND f.market_cap >= ?"
            params.append(min_market_cap * 1e9)
        if max_market_cap:
            query += " AND f.market_cap <= ?"
            params.append(max_market_cap * 1e9)
        if min_beta:
            query += " AND f.beta >= ?"
            params.append(min_beta)
        if max_beta:
            query += " AND f.beta <= ?"
            params.append(max_beta)
        if min_roe:
            query += " AND f.roe >= ?"
            params.append(min_roe / 100)
        if max_ev_ebitda:
            query += " AND f.ev_to_ebitda <= ?"
            params.append(max_ev_ebitda)
        if min_div_yield:
            query += " AND f.dividend_yield >= ?"
            params.append(min_div_yield / 100)
        if min_pct_from_low:
            query += " AND (p.price - f.week_52_low) / NULLIF(f.week_52_low, 0) * 100 >= ?"
            params.append(min_pct_from_low)
        if max_pct_from_high:
            query += " AND (p.price - f.week_52_high) / NULLIF(f.week_52_high, 0) * 100 >= ?"
            params.append(-abs(max_pct_from_high))

        valid_sorts = ["market_cap", "price", "change_1d_pct", "change_1w_pct", "change_1m_pct", "beta", "roe", "ev_to_ebitda", "volume"]
        if sort_by not in valid_sorts:
            sort_by = "market_cap"
        sort_col = f"f.{sort_by}" if sort_by in ["market_cap", "beta", "roe", "ev_to_ebitda"] else f"p.{sort_by}"
        query += f" ORDER BY {sort_col} {'DESC' if sort_dir == 'desc' else 'ASC'} NULLS LAST"
        query += " LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        cols = ["symbol","company_name","sector","industry","market_cap","beta",
                "week_52_high","week_52_low","roe","ev_to_ebitda","price_to_book",
                "price_to_sales","dividend_yield","roic","current_ratio","logo_url",
                "price","volume","change_1d_pct","change_1w_pct","change_1m_pct",
                "ma20","ma50","ma200","pct_from_52w_low","pct_from_52w_high","pct_from_ma50"]
        data = []
        for row in rows:
            d = dict(zip(cols, row))
            for k, v in d.items():
                if hasattr(v, 'item'):
                    d[k] = v.item()
            if d.get("market_cap"):
                d["market_cap_b"] = round(d["market_cap"] / 1e9, 2)
            data.append(d)
        conn.close()
        return {"data": data, "count": len(data), "source": "duckdb+fmp"}
    except Exception as e:
        log.error(f"Screener error: {e}")
        return {"data": [], "count": 0, "error": str(e)}

@router.get("/sectors")
def get_sectors():
    """Get all available sectors."""
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        rows = conn.execute("SELECT DISTINCT sector FROM agora.main.silver_equity_fundamentals WHERE sector IS NOT NULL ORDER BY sector").fetchall()
        conn.close()
        return {"sectors": [r[0] for r in rows]}
    except Exception as e:
        return {"sectors": [], "error": str(e)}