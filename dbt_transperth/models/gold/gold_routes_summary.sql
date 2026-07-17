with routes as (
    select * from {{ ref('silver_routes') }}
)
,trips as (
    select * from {{ ref('silver_trips') }}
)

select
    r.route_id
    ,r.route_short_name
    ,r.route_long_name
    ,r.route_type_label
    ,COUNT(t.trip_id) as total_trips
    ,SUM(CASE WHEN t.direction_id = 0 THEN 1 ELSE 0 END) as outbound_trips
    ,SUM(CASE WHEN t.direction_id = 1 THEN 1 ELSE 0 END) as inbound_trips
from
    routes r
    LEFT JOIN trips t
        ON r.route_id = t.route_id
WHERE TRUE
    AND r.route_id IS NOT NULL
group by
    r.route_id
    ,r.route_short_name
    ,r.route_long_name
    ,r.route_type_label