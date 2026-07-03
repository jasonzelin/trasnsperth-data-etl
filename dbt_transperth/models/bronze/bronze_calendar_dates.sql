with source as (
    select * from {{ ref('calendar_dates') }}
)

,raw as (
    select
        service_id
        ,date
        ,exception_type
        ,exception_label
    from
        source
)

select
    *
from
    raw