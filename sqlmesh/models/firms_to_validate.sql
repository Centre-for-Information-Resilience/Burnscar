MODEL (
  name arson.firms_to_validate,
  kind VIEW,
  description "FIRMS events pending validation.",
);

SELECT
  f.id AS firms_id,
  f.acq_date,
  ST_ASWKB(f.geom)::BLOB AS geom,
  ST_ASWKB(i.geom)::BLOB AS area_include_geom
FROM arson.firms AS f
JOIN arson.areas_include AS i
  ON ST_WITHIN(f.geom, i.geom)
WHERE f.acq_date < current_date - interval concat(@analyse_after_days, ' days')