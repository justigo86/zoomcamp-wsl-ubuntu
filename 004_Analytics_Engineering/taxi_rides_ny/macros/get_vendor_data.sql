-- Macro to generate vendor_name column using Jinja dictionary.

-- This approach works seamlessly across BigQuery, DuckDB, Snowflake, etc.
-- by generating a CASE statement at compile time.

-- Usage: {{ get_vendor_data('vendor_id') }}
-- Returns: SQL CASE expression that maps vendor_id to vendor_name

