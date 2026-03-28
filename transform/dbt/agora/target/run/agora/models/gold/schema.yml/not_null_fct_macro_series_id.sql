
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select series_id
from "agora"."main_gold"."fct_macro"
where series_id is null



  
  
      
    ) dbt_internal_test