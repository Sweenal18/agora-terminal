

WITH current_snapshot AS (
    SELECT *
    FROM "agora"."snapshots"."snap_instruments"
    WHERE dbt_valid_to IS NULL
),
final AS (
    SELECT
        md5(symbol)::VARCHAR                AS instrument_key,
        symbol,
        company_name,
        sector,
        industry,
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
        dbt_valid_from                      AS valid_from,
        snapshot_taken_at,
        fetched_at                          AS source_fetched_at
    FROM current_snapshot
)
SELECT * FROM final