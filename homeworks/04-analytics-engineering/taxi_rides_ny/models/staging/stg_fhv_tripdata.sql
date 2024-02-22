{{
    config(
        materialized='view'
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    TIMESTAMP_MILLIS(cast(pickup_datetime/ 1000000 as INT64)) as pickup_datetime,
    TIMESTAMP_MILLIS(cast(dropOff_datetime/ 1000000 as INT64)) as dropoff_datetime,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    SR_Flag as sr_flag,
    Affiliated_base_number as affiliated_base_number
from 
    {{ source('staging','fhv_tripdata') }}
where
    EXTRACT(YEAR from TIMESTAMP_MILLIS(cast(pickup_datetime/ 1000000 as INT64))) = 2019


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}
