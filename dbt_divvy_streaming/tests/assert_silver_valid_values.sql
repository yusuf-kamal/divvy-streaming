select *
from {{ ref('station_status') }}
where free_bikes < 0
   or empty_slots < 0
   or capacity <> free_bikes + empty_slots