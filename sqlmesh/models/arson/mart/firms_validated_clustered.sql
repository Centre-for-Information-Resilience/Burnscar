MODEL (
  kind VIEW,
  description 'Final output of the pipeline, clustered by area and date.',
  audits (
    NUMBER_OF_ROWS(threshold := 1)
  )
);

/* F */
SELECT
  fvc.*
  EXCLUDE (geom),
  ST_Y(fvc.geom)::DOUBLE AS latitude,
  ST_X(fvc.geom)::DOUBLE AS longitude,
  g.country_id,
  g.gadm_1,
  @IF(@gadm_level >= 2, g.gadm_2),
  @IF(@gadm_level >= 3, g.gadm_3),
  ng.settlement_name
FROM intermediate.firms_validated_clustered AS fvc
LEFT JOIN intermediate.nearest_geonames_firms_validated_clustered AS ng
  ON fvc.area_include_id = ng.area_include_id
JOIN reference.gadm AS g
  ON ST_INTERSECTS(fvc.geom, g.geom)
ORDER BY
  fvc.area_include_id,
  fvc.event_no,
  fvc.start_date