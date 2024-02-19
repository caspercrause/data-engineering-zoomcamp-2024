{{
    config(
        materialized='table'
    )
}}

with stg_fhv_tripdata as (
    select *,
    'For Hire Vehicles' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    stg_fhv_tripdata.tripid,
    stg_fhv_tripdata.service_type,
    stg_fhv_tripdata.dispatching_base_num,
    stg_fhv_tripdata.affiliated_base_number,
    stg_fhv_tripdata.pickup_locationid,
    stg_fhv_tripdata.dropoff_locationid,
    stg_fhv_tripdata.pickup_datetime,
    stg_fhv_tripdata.dropoff_datetime,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone

from stg_fhv_tripdata

inner join dim_zones as pickup_zone
    on stg_fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
    on stg_fhv_tripdata.dropoff_locationid = dropoff_zone.locationid