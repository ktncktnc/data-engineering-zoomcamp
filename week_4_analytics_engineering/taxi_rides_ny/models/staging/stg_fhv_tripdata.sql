{{ config(materialized='view') }}
 
select
   -- identifiers
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropOff_datetime,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationid as integer) as dropoff_locationid,
    
    -- timestamps
    SR_Flag,
    Affiliated_base_number,
from {{ source('staging','fhv') }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
