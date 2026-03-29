
      update "agora"."snapshots"."snap_instruments" as DBT_INTERNAL_TARGET
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "snap_instruments__dbt_tmp20260329032143069449" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = DBT_INTERNAL_TARGET.dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and DBT_INTERNAL_TARGET.dbt_valid_to is null;

    insert into "agora"."snapshots"."snap_instruments" ("symbol", "company_name", "sector", "industry", "exchange", "currency", "asset_class", "market_cap", "beta", "week_52_high", "week_52_low", "fetched_at", "snapshot_taken_at", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."symbol",DBT_INTERNAL_SOURCE."company_name",DBT_INTERNAL_SOURCE."sector",DBT_INTERNAL_SOURCE."industry",DBT_INTERNAL_SOURCE."exchange",DBT_INTERNAL_SOURCE."currency",DBT_INTERNAL_SOURCE."asset_class",DBT_INTERNAL_SOURCE."market_cap",DBT_INTERNAL_SOURCE."beta",DBT_INTERNAL_SOURCE."week_52_high",DBT_INTERNAL_SOURCE."week_52_low",DBT_INTERNAL_SOURCE."fetched_at",DBT_INTERNAL_SOURCE."snapshot_taken_at",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "snap_instruments__dbt_tmp20260329032143069449" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;


  