with activities as (
    select * from {{ ref('stg_geolife__activities') }}
)
select
  *
from
  activities
where
  not (latitude_deg between -90 and 90)
  or not (longitude_deg between -180 and 180)