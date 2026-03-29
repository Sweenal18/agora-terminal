insert into "agora"."main_gold"."fct_fundamentals" ("instrument_key", "date_key", "symbol", "snapshot_date", "market_cap", "beta", "week_52_high", "week_52_low", "roe", "ev_to_ebitda", "price_to_book", "price_to_sales", "dividend_yield", "roic", "current_ratio", "sector", "industry", "is_market_cap_missing", "is_roe_missing", "source_fetched_at", "dbt_loaded_at")
    (
        select "instrument_key", "date_key", "symbol", "snapshot_date", "market_cap", "beta", "week_52_high", "week_52_low", "roe", "ev_to_ebitda", "price_to_book", "price_to_sales", "dividend_yield", "roic", "current_ratio", "sector", "industry", "is_market_cap_missing", "is_roe_missing", "source_fetched_at", "dbt_loaded_at"
        from "fct_fundamentals__dbt_tmp20260329032353870398"
    )


  