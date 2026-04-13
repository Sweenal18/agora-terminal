"""
Asset Screener routes - queries Gold layer dimensional models
Joins dim_instruments + fct_prices + fct_fundamentals for filtering
"""
import os
import logging
import duckdb
import time
from threading import Lock
from fastapi import APIRouter

# Simple TTL cache
_cache = {}
_cache_lock = Lock()

def ttl_cache_get(key: str):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() < entry["expires"]:
            return entry["value"]
    return None

def ttl_cache_set(key: str, value, ttl_seconds: int):
    with _cache_lock:
        _cache[key] = {"value": value, "expires": time.time() + ttl_seconds}

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
    max_debt_to_equity: float = None,
    min_fcf_yield: float = None,
    only_up_days: bool = False,
    sort_by: str = "market_cap",
    sort_dir: str = "desc",
    limit: int = 50,
):
    """Screen stocks using Gold layer dimensional models."""
    cache_key = f"screen:{sector}:{market_cap_bucket}:{min_market_cap}:{max_pe}:{min_roe}:{sort_by}:{sort_dir}:{limit}:{symbol}:{max_debt_to_equity}:{min_fcf_yield}"
    cached = ttl_cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)

        query = """
            WITH latest_price AS (
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
                    debt_to_equity,
                    free_cash_flow_yield,
                    avg_volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY instrument_key
                        ORDER BY snapshot_date DESC
                    ) AS rn
                FROM agora.main_gold.fct_fundamentals
            ),
            moving_avgs AS (
                SELECT
                    instrument_key,
                    AVG(CASE WHEN rn <= 20  THEN price END) AS ma20,
                    AVG(CASE WHEN rn <= 50  THEN price END) AS ma50,
                    AVG(CASE WHEN rn <= 200 THEN price END) AS ma200,
                    MAX(CASE WHEN rn = 5  THEN price END) AS price_5d_ago,
                    MAX(CASE WHEN rn = 21 THEN price END) AS price_21d_ago
                FROM latest_price
                WHERE rn <= 200
                GROUP BY instrument_key
            )
            SELECT
                d.symbol,
                d.company_name,
                d.sector,
                d.industry,
                d.market_cap_bucket,
                d.asset_class,

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
                f.debt_to_equity,
                f.free_cash_flow_yield,
                f.avg_volume,

                p.price,
                p.volume,
                p.change_1d_pct,
                p.price_range_pct,
                p.volume_usd_approx,
                p.volume_ratio,
                p.is_up_day,
                p.trade_date AS last_date,

                m.ma20,
                m.ma50,
                m.ma200,

                (p.price - m.price_5d_ago)  / NULLIF(m.price_5d_ago, 0)  * 100 AS change_1w_pct,
                (p.price - m.price_21d_ago) / NULLIF(m.price_21d_ago, 0) * 100 AS change_1m_pct,

                CASE WHEN f.week_52_low > 0
                    THEN (p.price - f.week_52_low)  / f.week_52_low  * 100
                    ELSE NULL END AS pct_from_52w_low,
                CASE WHEN f.week_52_high > 0
                    THEN (p.price - f.week_52_high) / f.week_52_high * 100
                    ELSE NULL END AS pct_from_52w_high,

                CASE WHEN m.ma50 > 0
                    THEN (p.price - m.ma50) / m.ma50 * 100
                    ELSE NULL END AS pct_from_ma50

            FROM agora.main_gold.dim_instruments d
            LEFT JOIN latest_price        p ON d.instrument_key = p.instrument_key AND p.rn = 1
            LEFT JOIN latest_fundamentals f ON d.instrument_key = f.instrument_key AND f.rn = 1
            LEFT JOIN moving_avgs         m ON d.instrument_key = m.instrument_key
            WHERE d.asset_class = 'equity'
            AND d.is_current IS TRUE
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
        if max_debt_to_equity is not None:
            query += " AND f.debt_to_equity <= ?"
            params.append(max_debt_to_equity)
        if min_fcf_yield is not None:
            query += " AND f.free_cash_flow_yield >= ?"
            params.append(min_fcf_yield / 100)
        if only_up_days:
            query += " AND p.is_up_day = TRUE"

        valid_sorts = {
            "market_cap":           "f.market_cap",
            "price":                "p.price",
            "change_1d_pct":        "p.change_1d_pct",
            "change_1w_pct":        "(p.price - m.price_5d_ago) / NULLIF(m.price_5d_ago, 0) * 100",
            "volume":               "p.volume",
            "volume_ratio":         "p.volume_ratio",
            "beta":                 "f.beta",
            "roe":                  "f.roe",
            "ev_to_ebitda":         "f.ev_to_ebitda",
            "debt_to_equity":       "f.debt_to_equity",
            "free_cash_flow_yield": "f.free_cash_flow_yield",
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
            "debt_to_equity", "free_cash_flow_yield", "avg_volume",
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
        result = {"data": data, "count": len(data), "source": "duckdb+gold"}
        ttl_cache_set(cache_key, result, ttl_seconds=300)
        return result

    except Exception as e:
        log.error(f"Screener error: {e}")
        return {"data": [], "count": 0, "error": str(e)}


@router.get("/sectors")
def get_sectors():
    """Get all available sectors from dim_instruments."""
    cached = ttl_cache_get("sectors")
    if cached is not None:
        return cached
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        rows = conn.execute("""
            SELECT DISTINCT sector
            FROM agora.main_gold.dim_instruments
            WHERE sector IS NOT NULL
              AND sector != 'Unknown'
              AND is_current = true
            ORDER BY sector
        """).fetchall()
        conn.close()
        result = {"sectors": [r[0] for r in rows]}
        ttl_cache_set("sectors", result, ttl_seconds=3600)
        return result
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
              AND is_current = true
            GROUP BY market_cap_bucket
            ORDER BY count DESC
        """).fetchall()
        conn.close()
        return {"buckets": [{"bucket": r[0], "count": r[1]} for r in rows]}
    except Exception as e:
        return {"buckets": [], "error": str(e)}


@router.get("/search")
def search_assets(q: str = "", limit: int = 10):
    """
    Fuzzy search across symbol, company name, sector, and industry.
    Supports: ticker (AAPL), company name (Apple), sector (Technology).
    Results ranked: exact symbol match first, then starts-with, then contains.
    """
    if not q or len(q.strip()) < 1:
        return {"results": []}
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        results = conn.execute("""
            WITH scored AS (
                SELECT
                    d.symbol,
                    d.company_name,
                    d.sector,
                    d.industry,
                    d.asset_class,
                    d.market_cap_bucket,
                    p.close    AS price,
                    p.daily_return_pct AS change_pct,
                    CASE
                        WHEN UPPER(d.symbol)       = UPPER(?)       THEN 1
                        WHEN UPPER(d.symbol)       LIKE UPPER(?) || '%' THEN 2
                        WHEN UPPER(d.company_name) LIKE UPPER(?) || '%' THEN 3
                        WHEN UPPER(d.symbol)       LIKE '%' || UPPER(?) || '%' THEN 4
                        WHEN UPPER(d.company_name) LIKE '%' || UPPER(?) || '%' THEN 5
                        WHEN UPPER(d.sector)       LIKE '%' || UPPER(?) || '%' THEN 6
                        WHEN UPPER(d.industry)     LIKE '%' || UPPER(?) || '%' THEN 7
                        ELSE 99
                    END AS score
                FROM agora.main_gold.dim_instruments d
                LEFT JOIN (
                    SELECT instrument_key, close, daily_return_pct,
                           ROW_NUMBER() OVER (PARTITION BY instrument_key ORDER BY trade_date DESC) AS rn
                    FROM agora.main_gold.fct_prices
                ) p ON d.instrument_key = p.instrument_key AND p.rn = 1
                WHERE d.is_current = true
                  AND (
                    UPPER(d.symbol)       LIKE '%' || UPPER(?) || '%'
                    OR UPPER(d.company_name) LIKE '%' || UPPER(?) || '%'
                    OR UPPER(d.sector)       LIKE '%' || UPPER(?) || '%'
                    OR UPPER(d.industry)     LIKE '%' || UPPER(?) || '%'
                  )
            )
            SELECT symbol, company_name, sector, industry, asset_class,
                   market_cap_bucket, price, change_pct, score
            FROM scored
            WHERE score < 99
            ORDER BY score ASC, symbol ASC
            LIMIT ?
        """, [q, q, q, q, q, q, q, q, q, q, q, limit]).fetchall()

        conn.close()

        cols = ["symbol", "company_name", "sector", "industry", "asset_class",
                "market_cap_bucket", "price", "change_pct", "score"]
        data = []
        for row in results:
            d = dict(zip(cols, row))
            for k, v in d.items():
                if hasattr(v, "item"):
                    d[k] = v.item()
            data.append(d)

        return {"results": data, "query": q, "count": len(data)}

    except Exception as e:
        log.error(f"Search error: {e}")
        return {"results": [], "query": q, "error": str(e)}
@router.get("/peers/{symbol}")
def get_peers(symbol: str, limit: int = 6):
    """Get sector peers for a symbol from dim_instruments."""
    cache_key = f"peers:{symbol}:{limit}"
    cached = ttl_cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        # Get the sector for the requested symbol
        row = conn.execute("""
            SELECT sector FROM agora.main_gold.dim_instruments
            WHERE symbol = ? AND is_current = TRUE
        """, [symbol.upper()]).fetchone()

        if not row or not row[0] or row[0] == 'Unknown':
            conn.close()
            return {"symbol": symbol, "sector": None, "peers": []}

        sector = row[0]

        # Get peers in same sector, joined with latest price
        peers = conn.execute("""
            SELECT
                d.symbol,
                d.company_name,
                p.close,
                p.daily_return_pct
            FROM agora.main_gold.dim_instruments d
            LEFT JOIN (
                SELECT instrument_key, close, daily_return_pct,
                       ROW_NUMBER() OVER (PARTITION BY instrument_key ORDER BY trade_date DESC) AS rn
                FROM agora.main_gold.fct_prices
            ) p ON d.instrument_key = p.instrument_key AND p.rn = 1
            WHERE d.sector = ?
              AND d.is_current = TRUE
              AND d.symbol != ?
              AND p.close IS NOT NULL
            ORDER BY p.close DESC
            LIMIT ?
        """, [sector, symbol.upper(), limit]).fetchall()
        conn.close()

        result = {
            "symbol": symbol.upper(),
            "sector": sector,
            "peers": [
                {
                    "symbol": r[0],
                    "company_name": r[1],
                    "price": round(r[2], 2) if r[2] else None,
                    "change_pct": round(r[3], 4) if r[3] else None,
                }
                for r in peers
            ]
        }
        ttl_cache_set(cache_key, result, ttl_seconds=300)
        return result
    except Exception as e:
        return {"symbol": symbol, "sector": None, "peers": [], "error": str(e)}
