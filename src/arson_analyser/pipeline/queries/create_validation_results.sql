CREATE TABLE IF NOT EXISTS validation_results (
    firms_id INTEGER PRIMARY KEY,
    burn_scar_detected BOOLEAN,
    burnt_pixel_count INTEGER,
    burnt_building_count INTEGER,
    no_data BOOLEAN,
    too_cloudy BOOLEAN,
    FOREIGN KEY (firms_id) REFERENCES firms(id)
)