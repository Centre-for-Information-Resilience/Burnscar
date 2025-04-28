MODEL (
  name arson.settlements,
  kind FULL,
  description 'Settlements to relate FIRMS events to.'
);

SELECT
  featureNam::TEXT AS name,
  geom::GEOMETRY AS geom
FROM ST_READ(@path_settlements);

@CREATE_SPATIAL_INDEX(@this_model, geom)