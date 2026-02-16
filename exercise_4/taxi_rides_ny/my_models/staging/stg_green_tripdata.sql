select 

 cast(vendorid as integer) as vendor_id,
 cast(ratecodeid as integer) as rate_code_id,
 cast(pulocationid as integer) as pickup_location_id,
 cast(dolocationid as integer) as dropoff_location_id,


 cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
 cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

 store_and_fwd_flag,
 cast(passenger_count as integer) as passenger_count,
 cast(trip_distance as numeric) as trip_distance,

 cast(fare_amount as numeric) as fare_amount,
 cast(extra as numeric) as extra,
 cast(mta_tax as numeric) as mta_tax,
 cast(tip_amount as numeric) as tip_amount,
 cast(tolls_amount as numeric) as tolls_amount,
 cast(improvement_surcharge as numeric) as improvement_surcharge,
 cast(total_amount as numeric) as total_amount,
 cast(payment_type as integer) as payment_type,

 cast(ehail_fee as numeric) as ehail_fee,
 cast(trip_type as numeric) as trip_type

 from {{ source('staging_taxi_rides_ny','green_tripdata') }}