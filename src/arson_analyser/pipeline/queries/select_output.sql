SELECT f.id as firms_id,
    g.GID_0 as country_id,
    g.NAME_1,
    g.NAME_2,
    g.NAME_3,
    s.settlement_name,
    st_y(f.geom) AS latitude,
    st_x(f.geom) AS longitude,
    f.acq_date,
    c.area_include_id,
    c.event_no,
    v.no_data,
    v.too_cloudy,
    v.burn_scar_detected,
    v.burnt_pixel_count,
    v.burnt_building_count
FROM firms f
    JOIN validation_results v on f.id = v.firms_id
    JOIN clustered_events c ON f.area_include_id = c.area_include_id
    AND f.acq_date >= c.start_date
    AND f.acq_date <= c.end_date
    JOIN nearest_settlements s ON f.id = s.firms_id
    JOIN gadm g ON st_intersects(f.geom, g.geom)
ORDER BY f.area_include_id,
    f.acq_date;