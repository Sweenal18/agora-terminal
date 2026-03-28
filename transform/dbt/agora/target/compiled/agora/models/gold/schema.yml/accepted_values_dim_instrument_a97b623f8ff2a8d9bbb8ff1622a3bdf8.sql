
    
    

with all_values as (

    select
        market_cap_bucket as value_field,
        count(*) as n_records

    from "agora"."main_gold"."dim_instruments"
    group by market_cap_bucket

)

select *
from all_values
where value_field not in (
    'mega_cap','large_cap','mid_cap','small_cap','micro_cap','unknown'
)


