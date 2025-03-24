SELECT f.id,
    acq_date,
    st_aswkb(f.geom) as geom,
    st_aswkb(a.geom) as area_include_geom
FROM firms_joined f
    JOIN areas_include a ON f.area_include_id = a.id --     JOIN firms_status s ON f.id = s.id
    -- WHERE s.step = 'analysis' AND s.status IN ('pending', 'failed');