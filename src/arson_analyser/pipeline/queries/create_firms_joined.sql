CREATE TABLE IF NOT EXISTS firms_joined (id INTEGER, geom GEOMETRY, area_geom GEOMETRY);
CREATE INDEX IF NOT EXISTS firms_joined_geom_idx ON firms_joined USING RTREE (geom);
CREATE INDEX IF NOT EXISTS firms_joined_area_geom_idx ON firms_joined USING RTREE (area_geom);