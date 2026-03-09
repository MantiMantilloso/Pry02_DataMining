
    
    

with child as (
    select dolocationid as from_field
    from "ny_taxi_db"."public_silver"."silver_trips"
    where dolocationid is not null
),

parent as (
    select locationid as to_field
    from "ny_taxi_db"."bronze"."taxi_zone_lookup"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


