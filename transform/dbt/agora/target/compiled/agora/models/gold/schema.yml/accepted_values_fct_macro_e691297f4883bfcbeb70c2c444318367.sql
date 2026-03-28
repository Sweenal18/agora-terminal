
    
    

with all_values as (

    select
        series_category as value_field,
        count(*) as n_records

    from "agora"."main_gold"."fct_macro"
    group by series_category

)

select *
from all_values
where value_field not in (
    'interest_rate','yield_curve','inflation','employment','growth','risk_sentiment','money_supply','other'
)


