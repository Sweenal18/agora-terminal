
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unit
from "agora"."main_gold"."fct_macro"
where unit is null



  
  
      
    ) dbt_internal_test