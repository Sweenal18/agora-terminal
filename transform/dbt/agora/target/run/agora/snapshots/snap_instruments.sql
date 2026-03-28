
      
  
    
    

    create  table
      "agora"."snapshots"."snap_instruments"
  
    as (
      
    

    select *,
        md5(coalesce(cast(symbol as varchar ), '')
         || '|' || coalesce(cast(now()::timestamp as varchar ), '')
        ) as dbt_scd_id,
        now()::timestamp as dbt_updated_at,
        now()::timestamp as dbt_valid_from,
        
  
  coalesce(nullif(now()::timestamp, now()::timestamp), null)
  as dbt_valid_to
from (
        

    

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
        FROM "agora"."main"."silver_equity_ohlcv_daily"
    ) ohlcv
    LEFT JOIN "agora"."main"."silver_equity_fundamentals" f
        ON ohlcv.symbol = f.symbol

    ) sbq



    );
  
  
  