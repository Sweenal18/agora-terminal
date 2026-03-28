
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select industry
from "agora"."main_gold"."dim_instruments"
where industry is null



  
  
      
    ) dbt_internal_test