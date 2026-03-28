
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select market_cap_bucket
from "agora"."main_gold"."dim_instruments"
where market_cap_bucket is null



  
  
      
    ) dbt_internal_test