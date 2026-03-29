insert into "agora"."main_gold"."fct_macro" ("date_key", "series_id", "observation_date", "series_name", "unit", "indicator_value", "series_category", "reporting_frequency", "source_processed_at", "dbt_loaded_at")
    (
        select "date_key", "series_id", "observation_date", "series_name", "unit", "indicator_value", "series_category", "reporting_frequency", "source_processed_at", "dbt_loaded_at"
        from "fct_macro__dbt_tmp20260329032353834206"
    )


  