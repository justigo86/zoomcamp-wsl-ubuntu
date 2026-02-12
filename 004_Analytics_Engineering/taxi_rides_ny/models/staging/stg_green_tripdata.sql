SELECT *
FROM {{ source( 'raw_data', 'green_tripdata' )}}
LIMIT 20;

-- FROM {{ source( '[source_name]', '[table_name]' )}}