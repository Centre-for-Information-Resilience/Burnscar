WITH clusters AS (
    -- Step 1: Compute date differences
    SELECT id,
        acq_date,
        geom,
        area_include_id,
        LAG(acq_date) OVER (
            PARTITION BY area_include_id
            ORDER BY acq_date
        ) AS prev_date,
        acq_date - LAG(acq_date) OVER (
            PARTITION BY area_include_id
            ORDER BY acq_date
        ) AS date_diff
    FROM firms_joined
),
event_assignments AS (
    -- Step 2: Assign event IDs based on date gaps
    SELECT *,
        SUM(
            CASE
                WHEN date_diff > $max_date_gap
                OR date_diff IS NULL THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY area_include_id
            ORDER BY acq_date
        ) AS event_id
    FROM clusters
) -- Step 3: Group events per area & summarize
SELECT event_id::INTEGER as id,
    area_include_id,
    MIN(acq_date) AS start_date,
    MAX(acq_date) AS end_date,
    COUNT(*) AS event_count
FROM event_assignments
GROUP BY area_include_id,
    event_id
ORDER BY area_include_id,
    start_date;