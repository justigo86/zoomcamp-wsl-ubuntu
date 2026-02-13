WITH source AS (
    SELECT * FROM {{ source('raw_data', 'yellow_tripdata') }}
),

renamed AS (
    SELECT
        -- identifiers (standardized naming for consistency across yellow/green)
        CAST(vendorid AS integer) AS vendor_id,
        CAST(ratecodeid AS integer) AS rate_code_id,
        CAST(pulocationid AS integer) AS pickup_location_id,
        CAST(dolocationid AS integer) AS dropoff_location_id,

        -- timestamps (standardized naming)
        CAST(tpep_pickup_datetime AS timestamp) AS pickup_datetime,  -- tpep = Taxicab PASsenger Enhancement Program (yellow taxis)
        CAST(tpep_dropoff_datetime AS timestamp) AS dropoff_datetime,

        -- trip info
        CAST(store_and_fwd_flag AS string) AS store_and_fwd_flag,
        CAST(passenger_count AS integer) AS passenger_count,
        CAST(trip_distance AS numeric) AS trip_distance,
        1 AS trip_type, -- yellow taxis only have one trip_type (green hAS two)

        -- payment info
        CAST(fare_amount AS numeric) AS fare_amount,
        CAST(extra AS numeric) AS extra,
        CAST(mta_tax AS numeric) AS mta_tax,
        CAST(tip_amount AS numeric) AS tip_amount,
        CAST(tolls_amount AS numeric) AS tolls_amount,
        CAST(improvement_surcharge AS numeric) AS improvement_surcharge,
        0 AS ehail_fee, -- yellow taxis don't have ehail_fee (green do)
        CAST(total_amount AS numeric) AS total_amount,
        CAST(payment_type AS integer) AS payment_type

    FROM source
    -- Filter out records with null vendor_id (data quality requirement)
    WHERE vendorid IS NOT NULL
)

SELECT * FROM renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
WHERE pickup_datetime >= '2019-01-01' AND pickup_datetime < '2019-02-01'
{% endif %}