
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_margin_missing
from "agora"."main_gold"."fct_fundamentals"
where is_margin_missing is null



  
  
      
    ) dbt_internal_test