select station_id, source_timestamp, count(*) as n
from {{ ref('station_status') }}
group by station_id, source_timestamp
having count(*) > 1