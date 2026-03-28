
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        sector as value_field,
        count(*) as n_records

    from "agora"."main_gold"."dim_instruments"
    group by sector

)

select *
from all_values
where value_field not in (
    'Technology','Healthcare','Financials','Consumer Discretionary','Consumer Staples','Industrials','Energy','Utilities','Materials','Real Estate','Communication Services','Unknown'
)



  
  
      
    ) dbt_internal_test