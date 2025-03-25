CREATE SEQUENCE IF NOT EXISTS firms_id;
CREATE TABLE IF NOT EXISTS firms (
    id INTEGER DEFAULT nextval('firms_id') PRIMARY KEY,
    acq_date DATE NOT NULL,
    geom GEOMETRY NOT NULL,
    area_include_id VARCHAR(32),
    FOREIGN KEY (area_include_id) REFERENCES areas_include(id)
);
CREATE INDEX IF NOT EXISTS firms_geom_idx ON firms USING RTREE (geom);