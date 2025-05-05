MODEL (
  name arson.firms_validated,
  kind VIEW,
  audits (
    NUMBER_OF_ROWS(threshold := 1)
  )
);

SELECT
  *
FROM (
  @UNION('DISTINCT', @EACH([0, 1, 2], try_ -> arson.firms_validated_@{try_}))
)
ORDER BY
  acq_date