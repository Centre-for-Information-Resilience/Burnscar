MODEL (
  kind VIEW,
  description 'FIRMS events clustered by spatiotemporal proximity.',
  grain (
    event_no
  ),
  audits (
    NUMBER_OF_ROWS(threshold := 1)
  )
);

WITH RECURSIVE detections AS (
  SELECT
    f.id,
    f.acq_date,
    f.geom,
    v.before_date,
    v.after_date,
    v.burn_scar_detected,
    v.burnt_pixel_count,
    v.burnt_building_count,
    v.no_data,
    v.too_cloudy
  FROM intermediate.firms_validated AS v
  JOIN intermediate.firms AS f
    ON v.firms_id = f.id
), connected /* Recursive clustering by spatial + temporal proximity */ AS (
  /* Step 1: start each detection as its own cluster */
  SELECT
    id,
    id AS root_id
  FROM detections
  UNION
  /* Step 2: add all points near existing ones in space and time */
  SELECT
    d.id,
    c.root_id
  FROM detections AS d
  JOIN connected AS c
    ON d.id <> c.id
  JOIN detections AS d_ref
    ON d_ref.id = c.id
  WHERE
    ST_DWITHIN_SPHEROID(d.geom, d_ref.geom, @clustering_max_distance)
    AND ABS(DATE_DIFF('DAY', d.acq_date, d_ref.acq_date)) <= @clustering_max_date_gap
), clusters /* Assign final cluster number (event_no) */ AS (
  SELECT
    id,
    MIN(root_id) AS event_no
  FROM connected
  GROUP BY
    id
), event_assignments /* Add metadata to clustered events */ AS (
  SELECT
    d.*,
    c.event_no
  FROM detections AS d
  JOIN clusters AS c
    ON d.id = c.id
), distances /* Compute max distance within each event */ AS (
  SELECT
    a.event_no,
    MAX(ST_DISTANCE_SPHEROID(a.geom, b.geom)) AS max_distance
  FROM event_assignments AS a
  JOIN event_assignments AS b
    ON a.id < b.id AND a.event_no = b.event_no
  GROUP BY
    a.event_no
)
/* Final aggregated output */
SELECT
  ea.event_no::INT,
  ST_CENTROID(ST_UNION_AGG(ea.geom)) AS geom,
  ANY_VALUE(COALESCE(d.max_distance, 0)) AS max_distance,
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
FROM event_assignments AS ea
LEFT JOIN distances AS d
  USING (event_no)
GROUP BY
  ea.event_no
ORDER BY
  start_date