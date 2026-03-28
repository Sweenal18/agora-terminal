
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        asset_class as value_field,
        count(*) as n_records

    from "agora"."main_gold"."dim_instruments"
    group by asset_class

)

select *
from all_values
where value_field not in (
    'equity','crypto','etf','index'
)



  
  
      
    ) dbt_internal_test