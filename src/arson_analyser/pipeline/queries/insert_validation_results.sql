INSERT
    OR REPLACE INTO validation_results (
        firms_id,
        burn_scar_detected,
        burnt_pixel_count,
        burnt_building_count,
        no_data,
        too_cloudy
    )
VALUES (
        $firms_id,
        $burn_scar_detected,
        $burnt_pixel_count,
        $burnt_building_count,
        $no_data,
        $too_cloudy
    )