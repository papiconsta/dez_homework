
/*
    / one row per trip, with the revenue for that trip
    / add primary key to the table, which is the trip id (which is a combination of pickup datetime and location id)
    / Find all the duplicate trip ids and remove them from the table + understand why they are duplicated (is it a data issue or is it expected to have duplicate trip ids?)
    / FInd a way to enrich the column payment type.
*/

with mixed_tripdata as (
    select * from {{ ref("int_trips_unioned") }}
),

-- select * from green_tripdata
-- select * from mixed_tripdata limit 1

mixed_tripdata_with_id as (
    select
        *,
        concat(pickup_datetime, '_', pickup_location_id) as trip_id
    from mixed_tripdata
) 

select * from mixed_tripdata_with_id where payment_type = 6

