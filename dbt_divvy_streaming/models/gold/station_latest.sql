{{ config(materialized='table') }}

with ranked as (

    select
        *,
        row_number() over (
            partition by station_id
            order by source_timestamp desc, bronze_id desc
        ) as rn
    from {{ ref('station_status') }}

)

select
    station_id,
    station_name,
    latitude,
    longitude,
    free_bikes,
    empty_slots,
    capacity,
    source_timestamp,
    ingested_at
from ranked
where rn = 1