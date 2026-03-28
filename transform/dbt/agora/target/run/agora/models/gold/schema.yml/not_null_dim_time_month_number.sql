
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select month_number
from "agora"."main_gold"."dim_time"
where month_number is null



  
  
      
    ) dbt_internal_test