

WITH source AS (
    SELECT *
    FROM "agora"."main"."silver_equity_fundamentals"
    
        WHERE fetched_at::TIMESTAMP > (SELECT MAX(source_fetched_at) FROM "agora"."main_gold"."fct_fundamentals")
    
),
final AS (
    SELECT
        md5(symbol)::VARCHAR                                            AS instrument_key,
        CAST(STRFTIME(CAST(fetched_at::TIMESTAMP AS DATE), '%Y%m%d') AS INTEGER) AS date_key,
        symbol,
        CAST(fetched_at::TIMESTAMP AS DATE)                             AS snapshot_date,
        market_cap::DOUBLE                                              AS market_cap,
        beta::DOUBLE                                                    AS beta,
        week_52_high::DOUBLE                                            AS week_52_high,
        week_52_low::DOUBLE                                             AS week_52_low,
        roe::DOUBLE                                                     AS roe,
        ev_to_ebitda::DOUBLE                                            AS ev_to_ebitda,
        price_to_book::DOUBLE                                           AS price_to_book,
        price_to_sales::DOUBLE                                          AS price_to_sales,
        dividend_yield::DOUBLE                                          AS dividend_yield,
        roic::DOUBLE                                                    AS roic,
        current_ratio::DOUBLE                                           AS current_ratio,
        sector::VARCHAR                                                 AS sector,
        industry::VARCHAR                                               AS industry,
        (market_cap IS NULL)::BOOLEAN                                   AS is_market_cap_missing,
        (roe IS NULL)::BOOLEAN                                          AS is_roe_missing,
        fetched_at::TIMESTAMP                                           AS source_fetched_at,
        CURRENT_TIMESTAMP::TIMESTAMPTZ                                  AS dbt_loaded_at
    FROM source
)
SELECT * FROM final