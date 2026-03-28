insert into "agora"."main_gold"."fct_prices" ("instrument_key", "date_key", "symbol", "trade_date", "open", "high", "low", "close", "volume", "vwap", "trade_count", "daily_return_pct", "price_range", "price_range_pct", "volume_usd_approx", "volume_ratio", "is_up_day", "asset_class", "source", "adjusted", "dbt_loaded_at")
    (
        select "instrument_key", "date_key", "symbol", "trade_date", "open", "high", "low", "close", "volume", "vwap", "trade_count", "daily_return_pct", "price_range", "price_range_pct", "volume_usd_approx", "volume_ratio", "is_up_day", "asset_class", "source", "adjusted", "dbt_loaded_at"
        from "fct_prices__dbt_tmp20260328112030595032"
    )


  