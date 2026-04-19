{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = ['instrument_key', 'date_key'],
        tags                 = ['gold', 'fact']
    )
}}
WITH source AS (
    SELECT *
    FROM {{ source('silver_fundamentals', 'silver_equity_fundamentals') }}
    {% if is_incremental() %}
        WHERE updated_at::TIMESTAMP > (SELECT MAX(source_fetched_at) FROM {{ this }})
    {% endif %}
),
final AS (
    SELECT
        md5(symbol)::VARCHAR                                            AS instrument_key,
        CAST(STRFTIME(CAST(updated_at::TIMESTAMP AS DATE), '%Y%m%d') AS INTEGER) AS date_key,
        symbol,
        CAST(updated_at::TIMESTAMP AS DATE)                             AS snapshot_date,
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
        debt_to_equity::DOUBLE                                          AS debt_to_equity,
        free_cash_flow_yield::DOUBLE                                    AS free_cash_flow_yield,
        avg_volume::BIGINT                                              AS avg_volume,
        sector::VARCHAR                                                 AS sector,
        industry::VARCHAR                                               AS industry,
        exchange::VARCHAR                                               AS exchange,
        country::VARCHAR                                                AS country,
        (market_cap IS NULL)::BOOLEAN                                   AS is_market_cap_missing,
        (roe IS NULL)::BOOLEAN                                          AS is_roe_missing,
        updated_at::TIMESTAMP                                           AS source_fetched_at,
        CURRENT_TIMESTAMP::TIMESTAMPTZ                                  AS dbt_loaded_at
    FROM source
)
SELECT * FROM final