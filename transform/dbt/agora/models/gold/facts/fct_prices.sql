{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'append',
        unique_key           = ['instrument_key', 'date_key'],
        tags                 = ['gold', 'fact']
    )
}}

-- Grain: one row per (instrument_key, date_key).
-- Covers S&P 500 equities from Yahoo Finance / Polygon.
-- Incremental append: nightly Dagster run processes only the new day's rows.
-- Pre-computes derived metrics once here so no downstream consumer
-- ever has to recompute them: daily_return_pct, price_range_pct,
-- volume_usd_approx, volume_ratio, is_up_day.

WITH equity_raw AS (

    SELECT
        symbol,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
        COALESCE(vwap, close)    AS vwap,
        COALESCE(trade_count, 0) AS trade_count,
        source,
        adjusted,
        'equity'::VARCHAR        AS asset_class
    FROM {{ source('silver_equity', 'silver_equity_ohlcv_daily') }}

    {% if is_incremental() %}
        WHERE trade_date > (SELECT MAX(trade_date) FROM {{ this }})
    {% endif %}

),

with_keys AS (

    SELECT
        p.*,
        md5(p.symbol)::VARCHAR                              AS instrument_key,
        CAST(STRFTIME(p.trade_date, '%Y%m%d') AS INTEGER)   AS date_key
    FROM equity_raw p

),

with_prior_close AS (

    SELECT
        *,
        LAG(close)  OVER (PARTITION BY instrument_key ORDER BY trade_date) AS prior_close,
        LAG(volume) OVER (PARTITION BY instrument_key ORDER BY trade_date) AS prior_volume
    FROM with_keys

),

final AS (

    SELECT

        -- Keys
        instrument_key,
        date_key,

        -- Natural keys (retained for readability and direct queries)
        symbol,
        trade_date,

        -- OHLCV
        open,
        high,
        low,
        close,
        volume,
        vwap,
        trade_count,

        -- Daily return: % change from prior session close.
        -- NULL for first row of each instrument (no prior close).
        -- Expressed as percentage: 3.5 = +3.5%, not 0.035.
        ROUND(
            (close - prior_close) / NULLIF(prior_close, 0) * 100.0, 4
        )::DOUBLE                                           AS daily_return_pct,

        -- Intraday range (absolute)
        ROUND(high - low, 4)::DOUBLE                        AS price_range,

        -- Intraday range as % of open -- normalized volatility proxy.
        -- Comparable across instruments at different price levels.
        ROUND(
            (high - low) / NULLIF(open, 0) * 100.0, 4
        )::DOUBLE                                           AS price_range_pct,

        -- Approximate dollar volume traded. Useful for liquidity screening.
        ROUND(volume * vwap, 2)::DOUBLE                     AS volume_usd_approx,

        -- Volume vs prior day. >1.5 = elevated. >3.0 = spike.
        ROUND(
            volume::DOUBLE / NULLIF(prior_volume::DOUBLE, 0), 2
        )::DOUBLE                                           AS volume_ratio,

        -- Simple directional flag
        (close > open)::BOOLEAN                             AS is_up_day,

        -- Metadata
        asset_class,
        source,
        adjusted,
        CURRENT_TIMESTAMP::TIMESTAMPTZ                      AS dbt_loaded_at

    FROM with_prior_close

)

SELECT * FROM final