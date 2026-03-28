
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reporting_frequency
from "agora"."main_gold"."fct_macro"
where reporting_frequency is null



  
  
      
    ) dbt_internal_test