"""
Asset Screener routes - queries Gold layer dimensional models
Joins dim_instruments + fct_prices + fct_fundamentals for filtering
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
    market_cap_bucket: str = None,
    min_market_cap: float = None,
    max_market_cap: float = None,
    min_beta: float = None,
    max_beta: float = None,
    min_roe: float = None,
    max_ev_ebitda: float = None,
    min_div_yield: float = None,
    min_volume_ratio: float = None,
    only_up_days: bool = False,
    sort_by: str = "market_cap",
    sort_dir: str = "desc",
    limit: int = 50,
):
    """Screen stocks using Gold layer dimensional models."""
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)

        query = """
            WITH latest_price AS (
                -- Most recent price row per instrument from Gold fct_prices.
                -- daily_return_pct, volume_ratio, is_up_day are pre-computed by dbt.
                SELECT
                    instrument_key,
                    symbol,
                    trade_date,
                    open,
                    high,
                    low,
                    close            AS price,
                    volume,
                    vwap,
                    daily_return_pct AS change_1d_pct,
                    price_range_pct,
                    volume_usd_approx,
                    volume_ratio,
                    is_up_day,
                    ROW_NUMBER() OVER (
                        PARTITION BY instrument_key
                        ORDER BY trade_date DESC
                    ) AS rn
                FROM agora.main_gold.fct_prices
                WHERE asset_class = 'equity'
            ),
            latest_fundamentals AS (
                -- Most recent fundamentals snapshot per instrument.
                SELECT
                    instrument_key,
                    symbol,
                    market_cap,
                    beta,
                    week_52_high,
                    week_52_low,
                    roe,
                    ev_to_ebitda,
                    price_to_book,
                    price_to_sales,
                    dividend_yield,
                    roic,
                    current_ratio,
                    ROW_NUMBER() OVER (
                        PARTITION BY instrument_key
                        ORDER BY snapshot_date DESC
                    ) AS rn
                FROM agora.main_gold.fct_fundamentals
            ),
            moving_avgs AS (
                -- Moving averages computed from fct_prices history.
                SELECT
                    instrument_key,
                    AVG(CASE WHEN rn <= 20  THEN price END) AS ma20,
                    AVG(CASE WHEN rn <= 50  THEN price END) AS ma50,
                    AVG(CASE WHEN rn <= 200 THEN price END) AS ma200,
                    -- 1-week and 1-month returns using pre-computed daily_return_pct
                    MAX(CASE WHEN rn = 5  THEN price END) AS price_5d_ago,
                    MAX(CASE WHEN rn = 21 THEN price END) AS price_21d_ago
                FROM latest_price
                WHERE rn <= 200
                GROUP BY instrument_key
            )
            SELECT
                -- Instrument identity from dim_instruments
                d.symbol,
                d.company_name,
                d.sector,
                d.industry,
                d.market_cap_bucket,
                d.asset_class,

                -- Fundamentals from fct_fundamentals
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

                -- Latest price from fct_prices (pre-computed metrics)
                p.price,
                p.volume,
                p.change_1d_pct,
                p.price_range_pct,
                p.volume_usd_approx,
                p.volume_ratio,
                p.is_up_day,
                p.trade_date AS last_date,

                -- Moving averages
                m.ma20,
                m.ma50,
                m.ma200,

                -- Period returns
                (p.price - m.price_5d_ago)  / NULLIF(m.price_5d_ago, 0)  * 100 AS change_1w_pct,
                (p.price - m.price_21d_ago) / NULLIF(m.price_21d_ago, 0) * 100 AS change_1m_pct,

                -- 52-week position
                CASE WHEN f.week_52_low > 0
                    THEN (p.price - f.week_52_low)  / f.week_52_low  * 100
                    ELSE NULL END AS pct_from_52w_low,
                CASE WHEN f.week_52_high > 0
                    THEN (p.price - f.week_52_high) / f.week_52_high * 100
                    ELSE NULL END AS pct_from_52w_high,

                -- Distance from MA50
                CASE WHEN m.ma50 > 0
                    THEN (p.price - m.ma50) / m.ma50 * 100
                    ELSE NULL END AS pct_from_ma50

            FROM agora.main_gold.dim_instruments d
            LEFT JOIN latest_price        p ON d.instrument_key = p.instrument_key AND p.rn = 1
            LEFT JOIN latest_fundamentals f ON d.instrument_key = f.instrument_key AND f.rn = 1
            LEFT JOIN moving_avgs         m ON d.instrument_key = m.instrument_key
            WHERE d.asset_class = 'equity'
        """

        params = []
        if sector:
            query += " AND d.sector = ?"
            params.append(sector)
        if market_cap_bucket:
            query += " AND d.market_cap_bucket = ?"
            params.append(market_cap_bucket)
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
        if min_volume_ratio:
            query += " AND p.volume_ratio >= ?"
            params.append(min_volume_ratio)
        if only_up_days:
            query += " AND p.is_up_day = TRUE"

        valid_sorts = {
            "market_cap":    "f.market_cap",
            "price":         "p.price",
            "change_1d_pct": "p.change_1d_pct",
            "change_1w_pct": "(p.price - m.price_5d_ago) / NULLIF(m.price_5d_ago, 0) * 100",
            "volume":        "p.volume",
            "volume_ratio":  "p.volume_ratio",
            "beta":          "f.beta",
            "roe":           "f.roe",
            "ev_to_ebitda":  "f.ev_to_ebitda",
        }
        sort_expr = valid_sorts.get(sort_by, "f.market_cap")
        query += f" ORDER BY {sort_expr} {'DESC' if sort_dir == 'desc' else 'ASC'} NULLS LAST"
        query += " LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        cols = [
            "symbol", "company_name", "sector", "industry",
            "market_cap_bucket", "asset_class",
            "market_cap", "beta", "week_52_high", "week_52_low",
            "roe", "ev_to_ebitda", "price_to_book", "price_to_sales",
            "dividend_yield", "roic", "current_ratio",
            "price", "volume", "change_1d_pct", "price_range_pct",
            "volume_usd_approx", "volume_ratio", "is_up_day", "last_date",
            "ma20", "ma50", "ma200",
            "change_1w_pct", "change_1m_pct",
            "pct_from_52w_low", "pct_from_52w_high", "pct_from_ma50"
        ]
        data = []
        for row in rows:
            d = dict(zip(cols, row))
            for k, v in d.items():
                if hasattr(v, "item"):
                    d[k] = v.item()
            if d.get("market_cap"):
                d["market_cap_b"] = round(d["market_cap"] / 1e9, 2)
            data.append(d)
        conn.close()
        return {"data": data, "count": len(data), "source": "duckdb+gold"}

    except Exception as e:
        log.error(f"Screener error: {e}")
        return {"data": [], "count": 0, "error": str(e)}


@router.get("/sectors")
def get_sectors():
    """Get all available sectors from dim_instruments."""
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        rows = conn.execute("""
            SELECT DISTINCT sector
            FROM agora.main_gold.dim_instruments
            WHERE sector IS NOT NULL
              AND sector != 'Unknown'
            ORDER BY sector
        """).fetchall()
        conn.close()
        return {"sectors": [r[0] for r in rows]}
    except Exception as e:
        return {"sectors": [], "error": str(e)}


@router.get("/buckets")
def get_market_cap_buckets():
    """Get market cap bucket counts from dim_instruments."""
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        rows = conn.execute("""
            SELECT market_cap_bucket, COUNT(*) as count
            FROM agora.main_gold.dim_instruments
            WHERE asset_class = 'equity'
            GROUP BY market_cap_bucket
            ORDER BY count DESC
        """).fetchall()
        conn.close()
        return {"buckets": [{"bucket": r[0], "count": r[1]} for r in rows]}
    except Exception as e:
        return {"buckets": [], "error": str(e)}