MODEL (
  name arson.nearest_settlements,
  kind VIEW,
  description 'Nearest settlements to relate FIRMS events to.'
);

WITH distances AS (
  SELECT
    f.id AS firms_id,
    s.name AS settlement_name,
    ST_DISTANCE(f.geom, s.geom) AS distance
  FROM arson.firms_validated AS v
  JOIN arson.firms AS f
    ON v.firms_id = f.id
  CROSS JOIN arson.settlements AS s
), ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY firms_id ORDER BY distance ASC) AS rn
  FROM distances
)
/* F */
SELECT
  firms_id,
  settlement_name
FROM ranked
WHERE
  rn = 1