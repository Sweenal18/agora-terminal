
    
    

with all_values as (

    select
        currency as value_field,
        count(*) as n_records

    from "agora"."main_gold"."dim_instruments"
    group by currency

)

select *
from all_values
where value_field not in (
    'USD'
)


