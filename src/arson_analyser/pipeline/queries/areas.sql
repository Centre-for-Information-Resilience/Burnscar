CREATE INDEX IF NOT EXISTS areas_include_idx ON areas_include USING RTREE (geom);
CREATE OR REPLACE TABLE areas_include AS (
        SELECT md5(st_aswkb(geom)) AS id,
            st_makevalid(geom) AS geom
        FROM (
                SELECT unnest(st_dump(geom)).geom as geom
                FROM ST_Read('geo/include/urban_areas.gpkg')
            ) -- Unnest geometries because everything is in one massive multi geometry
    );