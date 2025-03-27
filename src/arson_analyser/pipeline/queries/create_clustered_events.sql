CREATE OR REPLACE TABLE clustered_events AS (
        WITH clusters AS (
            -- Step 1: Compute date differences
            SELECT id,
                acq_date,
                geom,
                area_include_id,
                burn_scar_detected,
                burnt_pixel_count,
                burnt_building_count,
                no_data,
                too_cloudy,
                LAG(acq_date) OVER (
                    PARTITION BY area_include_id
                    ORDER BY acq_date
                ) AS prev_date,
                acq_date - LAG(acq_date) OVER (
                    PARTITION BY area_include_id
                    ORDER BY acq_date
                ) AS date_diff
            FROM firms
                JOIN validation_results ON firms.id = validation_results.firms_id
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
        SELECT area_include_id,
            event_id::INTEGER as event_no,
            MIN(acq_date) AS start_date,
            MAX(acq_date) AS end_date,
            AVG(burn_scar_detected::INTEGER) as burn_scar_detected,
            AVG(burnt_pixel_count) as burnt_pixel_count,
            AVG(burnt_building_count) as burnt_building_count,
            AVG(no_data::INTEGER) as no_data,
            AVG(too_cloudy::INTEGER) as too_cloudy,
            COUNT(*) AS event_count
        FROM event_assignments
        GROUP BY area_include_id,
            event_id
        ORDER BY area_include_id,
            start_date
    )