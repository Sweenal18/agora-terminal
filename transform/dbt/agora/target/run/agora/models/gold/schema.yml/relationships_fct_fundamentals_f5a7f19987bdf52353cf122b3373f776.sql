
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select instrument_key as from_field
    from "agora"."main_gold"."fct_fundamentals"
    where instrument_key is not null
),

parent as (
    select instrument_key as to_field
    from "agora"."main_gold"."dim_instruments"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test