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
                'exchange',
                'currency'
            ]
        )
    }}

    SELECT
        ohlcv.symbol                                                    AS symbol,
        COALESCE(f.company_name, ohlcv.symbol)                          AS company_name,
        COALESCE(f.sector,   'Unknown')                                 AS sector,
        COALESCE(f.industry, 'Unknown')                                 AS industry,
        COALESCE(f.exchange,  'Unknown')                                AS exchange,
        'USD'::VARCHAR                                                  AS currency,
        'equity'::VARCHAR                                               AS asset_class,
        f.market_cap,
        f.beta,
        f.week_52_high,
        f.week_52_low,
        COALESCE(f.updated_at, CURRENT_TIMESTAMP::TIMESTAMP)            AS fetched_at,
        CURRENT_TIMESTAMP::TIMESTAMP                                    AS snapshot_taken_at

    FROM (
        SELECT DISTINCT symbol
        FROM {{ source('silver_equity', 'silver_equity_ohlcv_daily') }}
    ) ohlcv
    LEFT JOIN {{ source('silver_fundamentals', 'silver_equity_fundamentals') }} f
        ON ohlcv.symbol = f.symbol

{% endsnapshot %}