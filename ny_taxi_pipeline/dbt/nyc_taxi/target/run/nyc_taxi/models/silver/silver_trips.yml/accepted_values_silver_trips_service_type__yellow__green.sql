select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

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



      
    ) dbt_internal_test