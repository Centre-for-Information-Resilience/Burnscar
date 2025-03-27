CREATE OR REPLACE VIEW nearest_settlements AS (
        WITH distances AS (
            SELECT f.id as firms_id,
                s.name as settlement_name,
                ST_Distance(f.geom, s.geom) AS distance
            FROM validation_results AS v
                JOIN firms as f ON v.firms_id = f.id
                CROSS JOIN settlements AS s
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY firms_id
                    ORDER BY distance ASC
                ) AS rn
            FROM distances
        )
        SELECT firms_id,
            settlement_name
        FROM ranked
        WHERE rn = 1
    );