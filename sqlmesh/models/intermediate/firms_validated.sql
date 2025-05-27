MODEL (
  kind VIEW,
  audits (
    NUMBER_OF_ROWS(threshold := 1)
  )
);

SELECT
  v.*,
  f.geom AS geom
FROM (
  @UNION('DISTINCT', @EACH([0, 1, 2], try_ -> intermediate.firms_validated_@{try_}))
) AS v
JOIN intermediate.firms AS f
  ON v.firms_id = f.id
LEFT JOIN reference.areas_exclude AS e
  ON NOT ST_INTERSECTS(f.geom, e.geom)
WHERE
  e.geom IS NULL
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY v.firms_id ORDER BY v.validation_try DESC) = 1
ORDER BY
  v.acq_date