with source as (
    select * from {{ ref('stop_times') }}
)

,raw as (
    select
        trip_id
        ,arrival_time
        ,departure_time
        ,stop_id
        ,stop_sequence
        ,pickup_type
        ,drop_off_type
        ,timepoint
        ,fare
        ,zone
        ,section
    from
        source
)

select
    *
from
    raw