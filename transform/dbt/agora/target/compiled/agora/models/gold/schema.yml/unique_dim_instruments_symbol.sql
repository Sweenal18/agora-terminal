
    
    

select
    symbol as unique_field,
    count(*) as n_records

from "agora"."main_gold"."dim_instruments"
where symbol is not null
group by symbol
having count(*) > 1


