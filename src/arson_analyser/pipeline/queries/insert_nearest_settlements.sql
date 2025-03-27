WITH distances AS (
    SELECT f.id as firms_id,
        s.name as settlement_name,
        ST_Distance(f.geom, s.geom) AS distance
    FROM validation_results AS v
        JOIN firms as f ON v.firms_id = f.id
        CROSS JOIN settlements AS s
    WHERE f.id NOT IN (
            SELECT DISTINCT firms_id
            FROM nearest_settlements
        )
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY firms_id
            ORDER BY distance ASC
        ) AS rn
    FROM distances
)
INSERT INTO nearest_settlements
SELECT firms_id,
    settlement_name
FROM ranked
WHERE rn = 1;