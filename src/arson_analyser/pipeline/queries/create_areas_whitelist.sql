CREATE TABLE IF NOT EXISTS areas_whitelist (geom GEOMETRY NOT NULL);
CREATE INDEX IF NOT EXISTS areas_whitelist_idx ON areas_whitelist USING RTREE (geom);