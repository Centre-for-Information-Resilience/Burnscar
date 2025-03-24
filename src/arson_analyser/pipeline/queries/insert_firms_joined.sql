INSERT INTO firms_joined (id, acq_date, geom, area_include_id)
SELECT f.id,
    f.acq_date,
    f.geom,
    a.id as area_include_id
FROM firms f
    JOIN areas_include a ON ST_Within(f.geom, a.geom)
WHERE f.id NOT IN (
        SELECT DISTINCT id
        FROM firms_joined
    );