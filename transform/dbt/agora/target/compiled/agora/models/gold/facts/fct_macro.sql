

-- Note: silver_macro_indicators is populated by the Dagster macro fetcher.
-- If the table does not exist yet this model returns zero rows gracefully.

WITH source AS (
    SELECT *
    FROM "agora"."main"."silver_macro_indicators"
    
        WHERE processed_at > (SELECT MAX(dbt_loaded_at) FROM "agora"."main_gold"."fct_macro")
    
),
with_date_key AS (
    SELECT
        *,
        CAST(STRFTIME(CAST(observation_date AS DATE), '%Y%m%d') AS INTEGER) AS date_key
    FROM source
),
final AS (
    SELECT
        date_key,
        series_id,
        CAST(observation_date AS DATE)                          AS observation_date,
        series_name,
        unit,
        value                                                   AS indicator_value,
        CASE
            WHEN series_id IN ('DFF', 'FEDFUNDS')                          THEN 'interest_rate'
            WHEN series_id IN ('T10Y2Y', 'T10Y3M', 'T5Y5E')               THEN 'yield_curve'
            WHEN series_id IN ('T10YIE', 'CPIAUCSL', 'CPILFESL', 'PCEPI') THEN 'inflation'
            WHEN series_id IN ('UNRATE', 'ICSA', 'PAYEMS')                 THEN 'employment'
            WHEN series_id IN ('GDP', 'GDPC1', 'INDPRO')                   THEN 'growth'
            WHEN series_id IN ('VIXCLS', 'BAMLH0A0HYM2')                  THEN 'risk_sentiment'
            WHEN series_id IN ('M2SL', 'M1SL')                             THEN 'money_supply'
            ELSE 'other'
        END::VARCHAR                                            AS series_category,
        CASE
            WHEN series_id IN ('DFF','FEDFUNDS','T10Y2Y','T10Y3M','T10YIE','VIXCLS','BAMLH0A0HYM2') THEN 'daily'
            WHEN series_id IN ('CPIAUCSL','CPILFESL','UNRATE','ICSA','PAYEMS','M2SL','M1SL')         THEN 'monthly'
            WHEN series_id IN ('GDP','GDPC1','INDPRO')                                               THEN 'quarterly'
            ELSE 'unknown'
        END::VARCHAR                                            AS reporting_frequency,
        processed_at                                            AS source_processed_at,
        CURRENT_TIMESTAMP::TIMESTAMPTZ                          AS dbt_loaded_at
    FROM with_date_key
)
SELECT * FROM final