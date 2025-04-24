MODEL (
    name arson.output,
    kind VIEW,
    description 'Final output of the pipeline, including all relevant information for each FIRMS event.',
    enabled FALSE,
);

SELECT f.id as firms_id,
    st_y(f.geom) AS latitude,
    st_x(f.geom) AS longitude,
    f.acq_date,
    g.GID_0 as country_id,
    g.NAME_1,
    g.NAME_2,
    g.NAME_3,
    s.settlement_name,
    i.id as area_include_id,
    c.event_no,
    v.no_data,
    v.too_cloudy,
    v.burn_scar_detected,
    v.burnt_pixel_count,
    v.burnt_building_count
FROM arson.firms f
    JOIN arson.areas_include i ON ST_WITHIN(f.geom, i.geom)
    JOIN arson.firms_validated v on f.id = v.firms_id
    JOIN arson.firms_clustered c 
        on i.id = c.area_include_id
        AND f.acq_date >= c.start_date
        AND f.acq_date <= c.end_date
    JOIN arson.nearest_settlements s ON f.id = s.firms_id
    JOIN arson.gadm g ON st_intersects(f.geom, g.geom)
ORDER BY 
    i.id,
    c.event_no,
    f.acq_date