
    
    

with child as (
    select date_key as from_field
    from "agora"."main_gold"."fct_macro"
    where date_key is not null
),

parent as (
    select date_key as to_field
    from "agora"."main_gold"."dim_time"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


