CREATE OR REPLACE TABLE settlements AS (
        SELECT featureNam as name,
            geom
        from st_read($path_settlements)
    )