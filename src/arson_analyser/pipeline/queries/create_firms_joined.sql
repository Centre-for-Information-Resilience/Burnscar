CREATE TABLE IF NOT EXISTS firms_joined (
    id INTEGER,
    acq_date DATE,
    geom GEOMETRY,
    area_include_id VARCHAR(32),
    FOREIGN KEY (area_include_id) REFERENCES areas_include(id)
);
CREATE INDEX IF NOT EXISTS firms_joined_geom_idx ON firms_joined USING RTREE (geom);