-- Dimension table for NYC taxi zones
-- This is a simple pass-through from lookup seed file, but having it as a model
-- allows for future enhancements (e.g., adding calculated fields, filtering)

-- with taxi_zone_lookup as (
--   SELECT * FROM {{ ref('taxi_zone_lookup') }}
-- ),
-- renamed as (
--   select
--     locationid as location_id,
--     borough,
--     zone,
--     service_zone
--   from taxi_zone_lookup
-- )
-- SELECT * FROM renamed

select
    locationid as location_id,
    borough,
    zone,
    service_zone
from {{ ref('taxi_zone_lookup') }}