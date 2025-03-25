CREATE OR REPLACE TABLE gadm AS (
        SELECT *
        FROM st_read($gadm_path)
    );