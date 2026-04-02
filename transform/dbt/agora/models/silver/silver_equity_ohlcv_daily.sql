{{ config(materialized="view") }}
-- silver_equity_ohlcv_daily is now populated directly by Python ingestion scripts
-- (ingestion/fetch_sp500_ohlcv.py) writing into agora.main.silver_equity_ohlcv_daily
-- This model exists for lineage documentation only
SELECT
    symbol,
    trade_date,
    open,
    high,
    low,
    close,
    volume,
    vwap,
    trade_count,
    source,
    adjusted,
    processed_at
FROM agora.main.silver_equity_ohlcv_daily