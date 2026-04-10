{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'append',
        unique_key           = ['series_id', 'observation_date'],
        tags                 = ['gold', 'fact']
    )
}}
WITH source AS (
    SELECT *
    FROM {{ source('silver_macro', 'silver_macro_pulse') }}
    {% if is_incremental() %}
        WHERE fetched_at > (SELECT MAX(dbt_loaded_at) FROM {{ this }})
    {% endif %}
),
final AS (
    SELECT
        CAST(STRFTIME(CAST(date AS DATE), '%Y%m%d') AS INTEGER)  AS date_key,
        indicator                                                  AS series_id,
        indicator                                                  AS series_name,
        CAST(date AS DATE)                                         AS observation_date,
        NULL::VARCHAR                                              AS unit,
        TRY_CAST(value AS DOUBLE)                                  AS indicator_value,
        CASE
            WHEN indicator IN ('DFF', 'FEDFUNDS')                          THEN 'interest_rate'
            WHEN indicator IN ('T10Y2Y', 'T10Y3M', 'T5Y5E')               THEN 'yield_curve'
            WHEN indicator IN ('T10YIE', 'CPIAUCSL', 'CPILFESL', 'PCEPI') THEN 'inflation'
            WHEN indicator IN ('UNRATE', 'ICSA', 'PAYEMS')                 THEN 'employment'
            WHEN indicator IN ('GDP', 'GDPC1', 'INDPRO')                   THEN 'growth'
            WHEN indicator IN ('VIXCLS', 'BAMLH0A0HYM2')                  THEN 'risk_sentiment'
            WHEN indicator IN ('M2SL', 'M1SL')                             THEN 'money_supply'
            ELSE 'other'
        END::VARCHAR                                               AS series_category,
        CASE
            WHEN indicator IN ('DFF','FEDFUNDS','T10Y2Y','T10Y3M','T10YIE','VIXCLS','BAMLH0A0HYM2') THEN 'daily'
            WHEN indicator IN ('CPIAUCSL','CPILFESL','UNRATE','ICSA','PAYEMS','M2SL','M1SL')         THEN 'monthly'
            WHEN indicator IN ('GDP','GDPC1','INDPRO')                                               THEN 'quarterly'
            ELSE 'unknown'
        END::VARCHAR                                               AS reporting_frequency,
        fetched_at                                                 AS source_processed_at,
        CURRENT_TIMESTAMP::TIMESTAMPTZ                             AS dbt_loaded_at
    FROM source
)
SELECT * FROM final