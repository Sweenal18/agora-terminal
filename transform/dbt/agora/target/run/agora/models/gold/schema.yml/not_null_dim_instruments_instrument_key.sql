
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select instrument_key
from "agora"."main_gold"."dim_instruments"
where instrument_key is null



  
  
      
    ) dbt_internal_test