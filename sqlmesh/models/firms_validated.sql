MODEL (
  name arson.firms_validated,
  kind VIEW
);

SELECT
  *
FROM arson.firms_validated_try_1
UNION
(
  SELECT
    *
  FROM arson.firms_validated_try_2
)