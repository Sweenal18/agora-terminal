{{ config(materialized = 'table', tags = ['gold', 'dimension']) }}
WITH cdc_snapshot AS (
    SELECT *
    FROM {{ ref('snap_instruments') }}
),
cdc_instruments AS (
    SELECT
        md5(symbol)::VARCHAR                AS instrument_key,
        symbol,
        company_name,
        CASE
            WHEN symbol = 'GOOG'  THEN 'Communication Services'
            WHEN symbol = 'GOOGL' THEN 'Communication Services'
            ELSE sector
        END::VARCHAR AS sector,
        industry,
        exchange,
        currency,
        asset_class,
        market_cap,
        CASE
            WHEN market_cap >= 200000000000 THEN 'mega_cap'
            WHEN market_cap >=  10000000000 THEN 'large_cap'
            WHEN market_cap >=   2000000000 THEN 'mid_cap'
            WHEN market_cap >=    300000000 THEN 'small_cap'
            WHEN market_cap IS NOT NULL     THEN 'micro_cap'
            ELSE                                 'unknown'
        END::VARCHAR                        AS market_cap_bucket,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,
        dbt_valid_from                      AS valid_from,
        dbt_valid_to                        AS valid_to,
        snapshot_taken_at,
        fetched_at                          AS source_fetched_at
    FROM cdc_snapshot
),
fct_symbols AS (
    SELECT DISTINCT p.symbol
    FROM main_gold.fct_prices p
    LEFT JOIN cdc_snapshot c ON p.symbol = c.symbol AND c.dbt_valid_to IS NULL
    WHERE c.symbol IS NULL
),
extra_instruments AS (
    SELECT
        md5(s.symbol)::VARCHAR              AS instrument_key,
        s.symbol,
        COALESCE(f.company_name, s.symbol) AS company_name,
        COALESCE(f.sector, 'Unknown')       AS sector,
        COALESCE(f.industry, 'Unknown')     AS industry,
        COALESCE(f.exchange, 'Unknown')     AS exchange,
        'USD'::VARCHAR                      AS currency,
        'equity'::VARCHAR                   AS asset_class,
        f.market_cap,
        CASE
            WHEN f.market_cap >= 200000000000 THEN 'mega_cap'
            WHEN f.market_cap >=  10000000000 THEN 'large_cap'
            WHEN f.market_cap >=   2000000000 THEN 'mid_cap'
            WHEN f.market_cap >=    300000000 THEN 'small_cap'
            WHEN f.market_cap IS NOT NULL     THEN 'micro_cap'
            ELSE                                   'unknown'
        END::VARCHAR                        AS market_cap_bucket,
        TRUE                                AS is_current,
        CURRENT_TIMESTAMP                   AS valid_from,
        NULL::TIMESTAMPTZ                   AS valid_to,
        CURRENT_TIMESTAMP                   AS snapshot_taken_at,
        CURRENT_TIMESTAMP                   AS source_fetched_at
    FROM fct_symbols s
    LEFT JOIN {{ source('silver_fundamentals', 'silver_equity_fundamentals') }} f
        ON s.symbol = f.symbol
),
final AS (
    SELECT * FROM cdc_instruments
    UNION ALL
    SELECT * FROM extra_instruments
)
SELECT * FROM final