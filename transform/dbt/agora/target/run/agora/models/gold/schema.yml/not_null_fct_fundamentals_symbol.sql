
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select symbol
from "agora"."main_gold"."fct_fundamentals"
where symbol is null



  
  
      
    ) dbt_internal_test