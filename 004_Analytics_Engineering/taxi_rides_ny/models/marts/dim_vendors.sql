-- Dimension table for taxi technology vendors
-- Small static dimension defining vendor codes and their company names

-- with trips_unioned as (
--   SELECT * FROM {{ ref('int_trips_unioned') }}
-- ),
-- vendors as (
--   select
--     locationid as location_id,
--     borough,
--     zone,
--     service_zone
--   from trips_unioned
-- )
-- SELECT * FROM vendors



-- Dimension table for taxi technology vendors
-- Small static dimension defining vendor codes and their company names

with trips as (
    select * from {{ ref('fct_trips') }}
),

vendors as (
    select distinct
        vendor_id,
        {{ get_vendor_data('vendor_id') }} as vendor_name
    from trips
)

select * from vendors