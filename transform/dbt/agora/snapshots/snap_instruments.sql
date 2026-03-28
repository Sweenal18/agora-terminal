{% snapshot snap_instruments %}

    {{
        config(
            target_schema = 'snapshots',
            unique_key    = 'symbol',
            strategy      = 'check',
            check_cols    = [
                'company_name',
                'sector',
                'industry',
                'currency'
            ]
        )
    }}

    SELECT
        ohlcv.symbol                                                    AS symbol,
        COALESCE(f.company_name::VARCHAR, ohlcv.symbol)                 AS company_name,
        COALESCE(f.sector::VARCHAR,       'Unknown')                    AS sector,
        COALESCE(f.industry::VARCHAR,     'Unknown')                    AS industry,
        'USD'::VARCHAR                                                  AS currency,
        'equity'::VARCHAR                                               AS asset_class,
        f.market_cap::DOUBLE                                            AS market_cap,
        COALESCE(f.fetched_at::TIMESTAMP, CURRENT_TIMESTAMP::TIMESTAMP) AS fetched_at,
        CURRENT_TIMESTAMP::TIMESTAMP                                    AS snapshot_taken_at

    FROM (
        SELECT DISTINCT symbol
        FROM {{ source('silver_equity', 'silver_equity_ohlcv_daily') }}
    ) ohlcv
    LEFT JOIN {{ source('silver_fundamentals', 'silver_equity_fundamentals') }} f
        ON ohlcv.symbol = f.symbol

{% endsnapshot %}