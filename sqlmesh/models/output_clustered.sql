MODEL (
  name arson.output_clustered,
  kind VIEW,
  description 'Final output of the pipeline, clustered by area and date.',
  audits (
    NUMBER_OF_ROWS(threshold := 1)
  ),
);
WITH distances AS (
  SELECT
    fc.area_include_id,
    g.name AS settlement_name,
    ST_DISTANCE(fc.geom, g.geom) AS distance
  FROM arson.firms_validated_clustered AS fc
  LEFT JOIN arson.geonames AS g
    ON ST_DWithin(
      fc.geom,
      g.geom,
      300000  -- limit to 30km
    )
), ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY area_include_id ORDER BY distance ASC) AS rn
  FROM distances
), nearest_geonames as (SELECT
  area_include_id,
  settlement_name
FROM ranked
WHERE
  rn = 1)
/* F */
SELECT
  fc.* exclude (geom),
  ST_Y(fc.geom)::DOUBLE AS latitude,
  ST_X(fc.geom)::DOUBLE AS longitude,
  g.country_id,
  g.gadm_1,
  @IF(@gadm_level >= 2, g.gadm_2),
  @IF(@gadm_level >= 3, g.gadm_3),
  ng.settlement_name,
FROM arson.firms_validated_clustered AS fc
LEFT JOIN nearest_geonames AS ng
  ON fc.area_include_id = ng.area_include_id
JOIN arson.gadm AS g
  ON ST_INTERSECTS(fc.geom, g.geom)
ORDER BY
  fc.area_include_id,
  fc.event_no,
  fc.start_date