INSERT INTO areas_exclude (id, geom) (
        SELECT md5(st_aswkb(geom)) AS id,
            st_makevalid(geom)
        FROM (
                SELECT unnest(st_dump(geom)).geom as geom
                FROM ST_Read($path_areas_exclude)
            ) -- Unnest geometries because everything is in one massive multi geometry
        WHERE id NOT IN (
                SELECT DISTINCT id
                from areas_exclude
            ) -- deduplicate using geometry hash for consistency
    );