
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_market_cap_missing
from "agora"."main_gold"."fct_fundamentals"
where is_market_cap_missing is null



  
  
      
    ) dbt_internal_test