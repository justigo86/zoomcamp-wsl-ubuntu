WITH source AS (
    SELECT * FROM {{ source('raw_data', 'fhv_tripdata') }}
),

renamed AS (
    SELECT
        CAST(dispatching_base_num AS string) AS dispatching_num,
        CAST(pickup_datetime AS timestamp) AS pickup_datetime,
        CAST(dropoff_datetime AS timestamp) AS dropoff_datetime,
        CAST(pulocationid AS integer) AS pickup_location_id,
        CAST(dolocationid AS integer) AS dropoff_location_id,
        CAST(SR_Flag AS string) AS sr_flag,
        CAST(affiliated_base_number AS string) AS affiliated_num,
    FROM source
    -- Filter out records WITH null vendor_id (data quality requirement)
    WHERE dispatching_base_num  IS NOT NULL
)

SELECT * FROM renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}