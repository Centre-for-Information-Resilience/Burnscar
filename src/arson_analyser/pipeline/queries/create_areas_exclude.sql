CREATE TABLE IF NOT EXISTS areas_exclude (
    id VARCHAR(32) PRIMARY KEY,
    geom GEOMETRY NOT NULL
);
CREATE INDEX IF NOT EXISTS areas_include_idx ON areas_exclude USING RTREE (geom);