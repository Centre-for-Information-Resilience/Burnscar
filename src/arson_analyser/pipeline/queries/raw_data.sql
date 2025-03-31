CREATE OR REPLACE TABLE raw_data AS
SELECT *
FROM read_parquet(
                $path_raw_data,
                filename = false
        )