-- drop existing table
-- DROP TABLE IF EXISTS areas_include;
-- DROP SEQUENCE IF EXISTS areas_include_id;
-- recreate
-- CREATE SEQUENCE IF NOT EXISTS areas_include_id;
CREATE TABLE IF NOT EXISTS areas_include (
    -- id INTEGER DEFAULT nextval('areas_include_id') PRIMARY KEY,
    id VARCHAR(32) PRIMARY KEY,
    geom GEOMETRY NOT NULL
);
CREATE INDEX IF NOT EXISTS areas_include_idx ON areas_include USING RTREE (geom);