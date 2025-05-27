MODEL (
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column acq_date
  ),
  description 'Fires from the NASA FIRMS project, only necessary columns and coords are converted to geom',
  grain (longitude, latitude, acq_date)
);

SELECT
  (
    ROW_NUMBER() OVER (ORDER BY r.acq_date, r.acq_time, r.longitude, r.latitude)
  )::INT AS id, /* FIRMS identifier, ordered by datetime and coords */
  r.acq_date, /* acquisition date of the FIRMS detection */
  ST_POINT(r.longitude, r.latitude)::GEOMETRY AS geom /* point geometry of the FIRMS detection */
FROM staging.firms AS r
WHERE
  r.acq_date BETWEEN @start_ds AND @end_ds;

@CREATE_SPATIAL_INDEX(@this_model, geom)