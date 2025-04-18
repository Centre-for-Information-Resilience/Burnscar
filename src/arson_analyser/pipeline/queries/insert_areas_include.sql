INSERT INTO areas_include (id, geom) (
        SELECT md5(st_aswkb(geom)) AS id,
            geom
        FROM (
                SELECT st_makevalid(unnest(st_dump(geom)).geom) as geom
                FROM ST_Read($path_areas_include)
            ) -- Unnest geometries because everything is in one massive multi geometry
        WHERE id NOT IN (
                SELECT DISTINCT id
                from areas_include
            ) -- deduplicate using geometry hash for consistency
    );