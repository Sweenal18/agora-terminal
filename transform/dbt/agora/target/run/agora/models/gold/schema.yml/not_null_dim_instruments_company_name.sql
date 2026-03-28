
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select company_name
from "agora"."main_gold"."dim_instruments"
where company_name is null



  
  
      
    ) dbt_internal_test