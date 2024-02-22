{{
    config(
        materialized='table'
    )
}}

select
    fhv.tripid
    , 'FHV' as service_type
    , fhv.dispatching_base_num
    , fhv.pickup_datetime
    , fhv.dropoff_datetime
    , fhv.pickup_locationid
    , pickup_zone.borough as pickup_borough
    , pickup_zone.zone as pickup_zone
    , pickup_zone.service_zone as pickup_service_zone
    , fhv.dropoff_locationid
    , dropoff_zone.borough as dropoff_borough
    , dropoff_zone.zone as dropoff_zone
    , dropoff_zone.service_zone as dropoff_service_zone
    , fhv.sr_flag
    , fhv.affiliated_base_number
    , 
from 
    {{ ref('stg_fhv_tripdata') }} as fhv
inner join 
    {{ ref('dim_zones') }} as pickup_zone on 
        fhv.pickup_locationid = pickup_zone.locationid
        and pickup_zone.borough != 'Unknown'
inner join 
    {{ ref('dim_zones') }} as dropoff_zone on 
        fhv.dropoff_locationid = dropoff_zone.locationid
        and dropoff_zone.borough != 'Unknown'