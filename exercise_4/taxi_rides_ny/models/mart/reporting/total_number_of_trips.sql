select SUM(total_monthly_trips) as total_trips
from {{ ref('fct_monthly_zone_revenue') }}
Where service_type = 'Green' and revenue_month >= '2019-10-01'
    and revenue_month < '2019-11-01'
group by service_type 

-- select * from {{ ref('fct_monthly_zone_revenue') }} WHERE service_type = 'Green' and revenue_month >= '2019-10-01'
--     and revenue_month < '2019-11-01'