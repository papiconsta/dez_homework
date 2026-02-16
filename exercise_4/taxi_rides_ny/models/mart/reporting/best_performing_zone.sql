select
    pickup_zone,
    service_type,
    sum(revenue_monthly_total_amount) as total_revenue
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green'
    and revenue_month >= '2019-01-01'
    and revenue_month < '2021-01-01'
group by pickup_zone, service_type
order by total_revenue desc