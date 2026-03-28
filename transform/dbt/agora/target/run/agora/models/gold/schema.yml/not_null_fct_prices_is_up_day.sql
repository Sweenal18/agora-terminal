
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_up_day
from "agora"."main_gold"."fct_prices"
where is_up_day is null



  
  
      
    ) dbt_internal_test