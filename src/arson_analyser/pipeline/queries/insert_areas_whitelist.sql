INSERT INTO areas_whitelist (
        SELECT unnest(st_dump(geom)).geom as geom
        FROM ST_Read($path_areas_whitelist)
    );