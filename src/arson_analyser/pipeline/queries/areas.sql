CREATE OR REPLACE TABLE {table_name} AS (
        SELECT md5(st_aswkb(geom)) AS id,
            st_makevalid(geom) AS geom
        FROM (
                SELECT unnest(st_dump(geom)).geom as geom
                FROM ST_Read('{path}')
            ) -- Unnest geometries because everything is in one massive multi geometry
    );
CREATE INDEX IF NOT EXISTS {table_name} _geom_idx ON {table_name} USING RTREE (geom);