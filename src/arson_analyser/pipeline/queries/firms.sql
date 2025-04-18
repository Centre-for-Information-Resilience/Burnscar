CREATE SEQUENCE IF NOT EXISTS firms_id;
CREATE TABLE IF NOT EXISTS firms (
    id INTEGER DEFAULT nextval('firms_id') PRIMARY KEY,
    acq_date DATE NOT NULL,
    geom GEOMETRY NOT NULL,
    area_include_id VARCHAR(32),
    FOREIGN KEY (area_include_id) REFERENCES areas_include(id)
);
CREATE INDEX IF NOT EXISTS firms_geom_idx ON firms USING RTREE (geom);

CREATE TEMP TABLE raw_firms AS (
    SELECT acq_date,
        st_point(longitude, latitude) AS geom
    FROM read_parquet(
            @path_raw_data,
            filename = false
        )
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