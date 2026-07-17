with source as (
    select * from {{ ref('bronze_routes') }}
)

select
    route_id
    ,route_short_name
    ,route_long_name
    ,cast(route_type as integer) as route_type
    ,case cast(route_type as integer)
        when 2 then 'Rail'
        when 3 then 'Bus'
        when 4 then 'Ferry'
    end as route_type_label
from
    source