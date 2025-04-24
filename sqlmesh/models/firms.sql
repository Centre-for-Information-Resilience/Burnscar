MODEL (
  name arson.firms,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column acq_date,
  ),
  description 'Fires from the NASA FIRMS project, only necessary columns and coords are converted to geom',
  grain (longitude, latitude, acq_date)
);

SELECT
  (ROW_NUMBER() OVER (ORDER BY r.acq_date, r.longitude, r.latitude))::INT AS id,
  r.acq_date,
  ST_POINT(r.longitude, r.latitude)::GEOMETRY AS geom
FROM arson.firms_raw AS r
WHERE
  r.acq_date BETWEEN @start_ds AND @end_ds