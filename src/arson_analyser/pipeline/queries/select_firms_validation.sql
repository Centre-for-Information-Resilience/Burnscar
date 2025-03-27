SELECT f.id,
    acq_date,
    st_aswkb(f.geom) as geom,
    st_aswkb(a.geom) as area_include_geom
FROM firms f
    JOIN areas_include a ON f.area_include_id = a.id
    LEFT JOIN validation_results r ON f.id = r.firms_id
WHERE f.id NOT IN (
        SELECT DISTINCT firms_id
        FROM validation_results
    ) -- not present yet
    OR (
        (
            r.no_data = TRUE
            OR r.too_cloudy = TRUE
        ) -- no (usable) data
        AND (today() - f.acq_date < $retry_days) -- event date is less than retry_days days ago
    );