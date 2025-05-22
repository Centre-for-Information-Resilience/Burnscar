MODEL (
  name arson.firms_validated,
  kind VIEW,
  audits (
    NUMBER_OF_ROWS(threshold := 1)
  )
);

SELECT
  v.*,
  f.geom AS geom,
FROM (
  @UNION('DISTINCT', @EACH([0, 1, 2], try_ -> arson.firms_validated_@{try_}))
) as v
JOIN arson.firms AS f
  ON v.firms_id = f.id
ORDER BY
  v.acq_date