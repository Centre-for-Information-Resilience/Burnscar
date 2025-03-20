INSERT INTO firms (acq_date, geom) (
        SELECT acq_date,
            ST_POINT(longitude, latitude) AS geom
        FROM raw_data
        WHERE acq_date NOT IN (
                SELECT DISTINCT acq_date
                FROM firms
            )
    )