
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dbt_loaded_at
from "agora"."main_gold"."fct_prices"
where dbt_loaded_at is null



  
  
      
    ) dbt_internal_test