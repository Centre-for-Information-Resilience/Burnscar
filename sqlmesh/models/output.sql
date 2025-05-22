MODEL (
  name arson.output,
  kind VIEW,
  description 'Final output of the pipeline, including all relevant information for each FIRMS event.',
  audits (
    NUMBER_OF_ROWS(threshold := 1)
  ),
);

WITH distances AS (
  SELECT
    v.firms_id,
    g.name AS settlement_name,
    ST_DISTANCE(f.geom, g.geom) AS distance
  FROM arson.firms_validated AS v
  JOIN arson.firms AS f
    ON v.firms_id = f.id
  LEFT JOIN arson.geonames AS g
    ON ST_DWithin(
      f.geom,
      g.geom,
      30000  -- limit to 30km
    )
), ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY firms_id ORDER BY distance ASC) AS rn
  FROM distances
), nearest_geonames AS (
SELECT
  firms_id,
  settlement_name
FROM ranked
WHERE
  rn = 1)

/* F */
SELECT
  f.id AS firms_id,
  ST_Y(f.geom)::DOUBLE AS latitude,
  ST_X(f.geom)::DOUBLE AS longitude,
  f.acq_date,
  g.country_id,
  g.gadm_1,
  @IF(@gadm_level >= 2, g.gadm_2),
  @IF(@gadm_level >= 3, g.gadm_3),
  ng.settlement_name,
  i.id AS area_include_id,
  c.event_no,
  v.before_date,
  v.after_date,
  v.no_data,
  v.too_cloudy,
  v.burn_scar_detected,
  v.burnt_pixel_count,
  v.burnt_building_count
FROM arson.firms AS f
JOIN arson.areas_include AS i
  ON ST_INTERSECTS(f.geom, i.geom)
JOIN arson.firms_validated AS v
  ON f.id = v.firms_id
JOIN arson.firms_clustered AS c
  ON i.id = c.area_include_id
  AND f.acq_date >= c.start_date
  AND f.acq_date <= c.end_date
LEFT JOIN nearest_geonames AS ng
  ON f.id = ng.firms_id
JOIN arson.gadm AS g
  ON ST_INTERSECTS(f.geom, g.geom)
ORDER BY
  i.id,
  c.event_no,
  f.acq_date