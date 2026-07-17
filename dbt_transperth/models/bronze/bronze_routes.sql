with source as (
    select * from {{ source('google_transit', 'routes') }}
)

select
    *
from
    source