
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select close
from "agora"."main_gold"."fct_prices"
where close is null



  
  
      
    ) dbt_internal_test