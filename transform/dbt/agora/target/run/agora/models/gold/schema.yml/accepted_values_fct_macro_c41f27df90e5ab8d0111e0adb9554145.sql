
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        reporting_frequency as value_field,
        count(*) as n_records

    from "agora"."main_gold"."fct_macro"
    group by reporting_frequency

)

select *
from all_values
where value_field not in (
    'daily','monthly','quarterly','unknown'
)



  
  
      
    ) dbt_internal_test