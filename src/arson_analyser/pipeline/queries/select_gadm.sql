SELECT v.firms_id,
    g.NAME_1,
    g.NAME_2,
    g.NAME_3
FROM validation_results v
    JOIN firms f ON v.firms_id = f.id
    JOIN gadm g ON st_intersects(f.geom, g.geom);