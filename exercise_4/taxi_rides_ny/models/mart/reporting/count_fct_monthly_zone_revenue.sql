select count(*) as record_count from (
    select
        coalesce(z.zone, 'Unknown Zone') as pickup_zone,
        {% if target.type == 'bigquery' %}cast(date_trunc(t.pickup_datetime, month) as date)
        {% elif target.type == 'duckdb' %}date_trunc('month', t.pickup_datetime)
        {% endif %} as revenue_month,
        t.service_type
    from {{ ref('int_trips_unioned') }} t
    left join {{ ref('dim_zones') }} z
        on t.pickup_location_id = z.location_id
    group by 1, 2, 3
)
