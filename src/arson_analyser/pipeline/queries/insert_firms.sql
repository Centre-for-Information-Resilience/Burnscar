CREATE TEMP TABLE raw_firms AS (
    SELECT acq_date,
        st_point(longitude, latitude) AS geom
    FROM raw_data
    WHERE acq_date NOT IN (
            SELECT DISTINCT acq_date
            FROM firms
        )
);
CREATE INDEX IF NOT EXISTS raw_firms_geom_idx ON raw_firms USING RTREE (geom);
INSERT INTO firms (acq_date, geom, area_include_id)
SELECT r.acq_date,
    r.geom,
    i.id as area_include_id
FROM raw_firms r
    JOIN areas_include i ON st_within(r.geom, i.geom)
    JOIN areas_exclude x ON NOT st_within(r.geom, x.geom);