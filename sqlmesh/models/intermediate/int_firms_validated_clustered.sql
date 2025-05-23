MODEL (
  name arson.int_firms_validated_clustered,
  kind VIEW,
  description 'FIRMS events clustered by area and date.',
  grain (area_include_id, event_no),
  audits (
    NUMBER_OF_ROWS(threshold := 1)
  ),
);

WITH clusters AS (
  /* Step 1: Compute date differences */
  SELECT
    f.id,
    f.acq_date,
    f.geom,
    i.id AS area_include_id,
    v.before_date,
    v.after_date,
    v.burn_scar_detected,
    v.burnt_pixel_count,
    v.burnt_building_count,
    v.no_data,
    v.too_cloudy,
    LAG(f.acq_date) OVER (PARTITION BY i.id ORDER BY f.acq_date) AS prev_date,
    f.acq_date - LAG(f.acq_date) OVER (PARTITION BY i.id ORDER BY f.acq_date) AS date_diff
  FROM arson.int_firms_validated as v
  JOIN arson.int_firms AS f
    ON v.firms_id = f.id
  JOIN arson.ref_areas_include AS i
    ON ST_INTERSECTS(f.geom, i.geom)
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
),
distances AS (
  SELECT
    a.area_include_id,
    a.event_no,
    MAX(ST_Distance(@geo_transform(a.geom),
        @geo_transform(b.geom))) AS max_distance,
  FROM event_assignments a
  JOIN event_assignments b
    ON a.area_include_id = b.area_include_id
    AND a.event_no = b.event_no
    AND a.id < b.id 
  GROUP BY a.area_include_id, a.event_no
)

SELECT
  ea.area_include_id,
  ea.event_no::INT,
  ST_Centroid(ST_Union_Agg(ea.geom)) AS geom,
  COALESCE(MAX(d.max_distance), 0) AS max_distance,
  MIN(ea.acq_date) AS start_date,
  MAX(ea.acq_date) AS end_date,
  MODE(ea.before_date) AS before_date,
  MODE(ea.after_date) AS after_date,
  AVG(ea.burn_scar_detected::INT) AS burn_scar_detected,
  AVG(ea.burnt_pixel_count) AS burnt_pixel_count,
  AVG(ea.burnt_building_count) AS burnt_building_count,
  AVG(ea.no_data::INT) AS no_data,
  AVG(ea.too_cloudy::INT) AS too_cloudy,
  COUNT(*) AS event_count
FROM event_assignments ea
LEFT JOIN distances d
  ON ea.area_include_id = d.area_include_id
  AND ea.event_no = d.event_no
GROUP BY
  area_include_id,
  event_no
ORDER BY
  area_include_id,
  start_date