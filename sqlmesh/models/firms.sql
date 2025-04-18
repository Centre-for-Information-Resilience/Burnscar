MODEL (
  name arson.firms,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column acq_date
  ),
  start '2025-01-01',
  cron '@daily',
  grain (latitude, longitude, acq_date)
);

SELECT
  r.acq_date,
  r.geom,
  i.id AS area_include_id
FROM arson.raw_firms AS r
JOIN arson.areas_include AS i
  ON ST_WITHIN(r.geom, i.geom)