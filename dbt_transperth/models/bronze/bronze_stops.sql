with source as (
    select * from {{ ref('stops') }}
)

,raw as (
    select
        location_type
        ,parent_station
        ,stop_id
        ,stop_code
        ,stop_name
        ,stop_desc
        ,stop_lat
        ,stop_lon
        ,zone_id
        ,supported_modes
        ,iptiscode
        ,_has_coordinates
    from
        source
)

select
    *
from
    raw