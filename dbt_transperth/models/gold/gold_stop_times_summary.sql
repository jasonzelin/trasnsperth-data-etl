with stop_times as (
    select * from {{ ref('silver_stop_times') }}
)

,stops as (
    select * from {{ ref('silver_stops') }}
)

select
    s.stop_id
    ,s.stop_name
    ,s.supported_modes
    ,s.stop_lat
    ,s.stop_lon
    ,COUNT(st.arrival_time) as total_arrivals
from
    stops s
    LEFT JOIN stop_times st
        ON s.stop_id = st.stop_id
WHERE TRUE
    AND s.stop_id IS NOT NULL
    AND s.supported_modes IS NOT NULL
group by
    s.stop_id
    ,s.stop_name
    ,s.supported_modes
    ,s.stop_lat
    ,s.stop_lon