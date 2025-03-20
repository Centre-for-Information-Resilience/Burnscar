CREATE SEQUENCE IF NOT EXISTS firms_id;
CREATE TABLE IF NOT EXISTS firms (
    id INTEGER DEFAULT nextval('firms_id'),
    acq_date DATE NOT NULL,
    geom GEOMETRY NOT NULL,
);
CREATE INDEX IF NOT EXISTS firms_geom_idx ON firms USING RTREE (geom);