
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sector
from "agora"."main_gold"."dim_instruments"
where sector is null



  
  
      
    ) dbt_internal_test