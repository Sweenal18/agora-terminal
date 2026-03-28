
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select series_category
from "agora"."main_gold"."fct_macro"
where series_category is null



  
  
      
    ) dbt_internal_test