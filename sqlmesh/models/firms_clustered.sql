MODEL (
  name arson.firms_clustered,
  kind VIEW,
  description 'FIRMS events clustered by area and date.',
  grain (area_include_id, event_no)
);

WITH clusters AS (
  /* Step 1: Compute date differences */
  SELECT
    f.id,
    f.acq_date,
    f.geom,
    i.id AS area_include_id,
    v.burn_scar_detected,
    v.burnt_pixel_count,
    v.burnt_building_count,
    v.no_data,
    v.too_cloudy,
    LAG(f.acq_date) OVER (PARTITION BY i.id ORDER BY f.acq_date) AS prev_date,
    f.acq_date - LAG(f.acq_date) OVER (PARTITION BY i.id ORDER BY f.acq_date) AS date_diff
  FROM arson.firms AS f
  JOIN arson.areas_include AS i
    ON ST_WITHIN(f.geom, i.geom)
  JOIN arson.firms_validated AS v
    ON f.id = v.firms_id
), event_assignments /* Step 3: Group events per area & summarize */ AS (
  /* Step 2: Assign event IDs based on date gaps */
  SELECT
    *,
    SUM(
      CASE
        WHEN date_diff > @clustering_max_date_gap OR date_diff IS NULL
        THEN 1
        ELSE 0
      END
    ) OVER (PARTITION BY area_include_id ORDER BY acq_date) AS event_no
  FROM clusters
)
SELECT
  area_include_id,
  event_no::INT,
  MIN(acq_date) AS start_date,
  MAX(acq_date) AS end_date,
  AVG(burn_scar_detected::INT) AS burn_scar_detected,
  AVG(burnt_pixel_count) AS burnt_pixel_count,
  AVG(burnt_building_count) AS burnt_building_count,
  AVG(no_data::INT) AS no_data,
  AVG(too_cloudy::INT) AS too_cloudy,
  COUNT(*) AS event_count
FROM event_assignments
GROUP BY
  area_include_id,
  event_no
ORDER BY
  area_include_id,
  start_date