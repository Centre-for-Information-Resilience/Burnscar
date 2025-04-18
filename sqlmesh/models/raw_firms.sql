MODEL (
  name arson.raw_firms,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column acq_date
  ),
  start '2025-01-01',
  cron '@daily',
  grain (latitude, longitude, acq_date)
);

SELECT
  acq_date::DATE AS acq_date,
  ST_POINT(longitude, latitude)::GEOMETRY AS geom
FROM READ_PARQUET(@path_raw_data, filename = FALSE)
WHERE
  acq_date BETWEEN @start_date AND @end_date