with source as (
    select * from {{ ref('calendar') }}
)

,raw as (
    select
        service_id
        ,monday
        ,tuesday
        ,wednesday
        ,thursday
        ,friday
        ,saturday
        ,sunday
        ,start_date
        ,end_date
    from
        source
)

select
    *
from
    raw