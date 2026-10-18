{{ config(materialized='table') }}

select
    station_id,
    dateadd(hour, datediff(hour, 0, source_timestamp), 0) as hour_start,
    avg(cast(free_bikes as float))                        as avg_free_bikes,
    avg(cast(empty_slots as float))                       as avg_empty_slots,
    min(free_bikes)                                       as min_free_bikes,
    max(free_bikes)                                       as max_free_bikes,
    max(capacity)                                         as capacity,
    count(*)                                              as updates
from {{ ref('station_status') }}
group by
    station_id,
    dateadd(hour, datediff(hour, 0, source_timestamp), 0)