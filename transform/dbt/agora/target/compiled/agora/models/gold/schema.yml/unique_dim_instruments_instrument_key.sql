
    
    

select
    instrument_key as unique_field,
    count(*) as n_records

from "agora"."main_gold"."dim_instruments"
where instrument_key is not null
group by instrument_key
having count(*) > 1


