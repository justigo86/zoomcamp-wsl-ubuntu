-- Dimension table for taxi technology vendors
-- Small static dimension defining vendor codes and their company names

with trips_unioned as (
  SELECT * FROM {{ ref('int_trips_unioned') }}
),
vendors as (
  select
    locationid as location_id,
    borough,
    zone,
    service_zone
  from trips_unioned
)
SELECT * FROM vendors