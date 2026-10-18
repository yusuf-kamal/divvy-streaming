{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        as_columnstore=false,
        post_hook="if not exists (select 1 from sys.indexes where name = 'UX_silver_station_source' and object_id = object_id('{{ this }}')) create unique index UX_silver_station_source on {{ this }} (station_id, source_timestamp)"
    )
}}

with source as (

    select *
    from {{ source('bronze', 'station_status') }}
    {% if is_incremental() %}
    -- only rows that arrived in bronze since the last run
    where id > (select coalesce(max(bronze_id), 0) from {{ this }})
    {% endif %}

),

cleaned as (

    select
        id                                   as bronze_id,
        network_id,
        station_id,
        nullif(ltrim(rtrim(name)), '')       as station_name,
        latitude,
        longitude,
        free_bikes,
        empty_slots,
        free_bikes + empty_slots             as capacity,
        source_timestamp,
        ingested_at,
        loaded_at
    from source
    where station_id is not null
      and source_timestamp is not null
      and latitude  between -90  and 90
      and longitude between -180 and 180
      and free_bikes  >= 0
      and empty_slots >= 0

),

deduped as (

    select
        *,
        row_number() over (
            partition by station_id, source_timestamp
            order by ingested_at, bronze_id
        ) as rn
    from cleaned

)

select
    bronze_id,
    network_id,
    station_id,
    station_name,
    latitude,
    longitude,
    free_bikes,
    empty_slots,
    capacity,
    source_timestamp,
    ingested_at,
    loaded_at
from deduped d
where rn = 1
{% if is_incremental() %}
  and not exists (
      select 1
      from {{ this }} t
      where t.station_id = d.station_id
        and t.source_timestamp = d.source_timestamp
  )
{% endif %}