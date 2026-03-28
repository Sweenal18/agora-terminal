
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_market_holiday
from "agora"."main_gold"."dim_time"
where is_market_holiday is null



  
  
      
    ) dbt_internal_test