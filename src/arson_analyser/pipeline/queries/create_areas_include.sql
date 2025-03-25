CREATE TABLE IF NOT EXISTS areas_include (
    id VARCHAR(32) PRIMARY KEY,
    geom GEOMETRY NOT NULL
);
CREATE INDEX IF NOT EXISTS areas_include_idx ON areas_include USING RTREE (geom);