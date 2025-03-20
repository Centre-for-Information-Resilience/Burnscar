CREATE TABLE IF NOT EXISTS raw_data AS (
    SELECT *
    FROM read_parquet($path_raw_data, filename = false)
    LIMIT 0 -- we infer the schema from the parquet files.
);