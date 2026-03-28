
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select series_name
from "agora"."main_gold"."fct_macro"
where series_name is null



  
  
      
    ) dbt_internal_test