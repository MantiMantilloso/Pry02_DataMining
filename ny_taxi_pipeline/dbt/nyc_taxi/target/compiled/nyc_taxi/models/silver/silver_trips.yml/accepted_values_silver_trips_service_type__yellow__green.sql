
    
    

with all_values as (

    select
        service_type as value_field,
        count(*) as n_records

    from "ny_taxi_db"."public_silver"."silver_trips"
    group by service_type

)

select *
from all_values
where value_field not in (
    'yellow','green'
)


