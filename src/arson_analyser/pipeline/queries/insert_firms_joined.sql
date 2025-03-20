INSERT INTO firms_joined (id, geom, area_geom)
SELECT f.id,
    f.geom,
    a.geom as area_geom
FROM firms f
    JOIN areas_whitelist a ON ST_Within(f.geom, a.geom)
WHERE f.id NOT IN (
        SELECT DISTINCT id
        FROM firms_joined
    );