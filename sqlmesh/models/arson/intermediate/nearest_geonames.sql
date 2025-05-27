MODEL (
  name intermediate.nearest_geonames_@source_table,
  description 'Find the nearest geoname settlement for each FIRMS event.',
  kind VIEW,
  blueprints (
    (source_table := firms_validated_clustered, id_col := area_include_id),
    (source_table := firms_validated, id_col := firms_id)
  ),
  audits (
    NUMBER_OF_ROWS(threshold := 1)
  )
);

WITH distances AS (
  SELECT
    s.@id_col,
    g.name AS settlement_name,
    ST_DISTANCE(s.geom, g.geom) AS distance
  FROM intermediate.@source_table AS s
  LEFT JOIN reference.geonames AS g
    ON ST_DWITHIN(s.geom, g.geom, 300000 /* limit to 30km */)
), ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY @id_col ORDER BY distance ASC) AS rn
  FROM distances
)
SELECT
  @id_col, /* Column to match FIRMS detection or cluster on */
  settlement_name /* Name of the nearest geoname settlement */
FROM ranked
WHERE
  rn = 1