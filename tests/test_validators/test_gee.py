from pathlib import Path

import pandas as pd
import pytest

from burnscar.models import FireDetection
from burnscar.validators.gee import GEEValidator, ValidationResult

PROJECT_ROOT = Path(__file__).parent.parent.parent
TEST_DATA = PROJECT_ROOT / "output" / "validation"

rows = pd.read_csv(
    TEST_DATA / "original" / "input.csv",
)
rows["geom"] = rows["geom"].apply(
    lambda x: x.encode("utf-8").decode("unicode_escape").encode("latin1")
)
rows["area_include_geom"] = rows["area_include_geom"].apply(
    lambda x: x.encode("utf-8").decode("unicode_escape").encode("latin1")
)

detections = []
for d in rows.to_dict(orient="records"):
    detections.append(FireDetection.model_validate(d))


@pytest.fixture(scope="module")
def validator():
    key_path = PROJECT_ROOT / "key.json"
    return GEEValidator(key_path=key_path)


@pytest.fixture
def expected_results():
    results = pd.read_csv(
        TEST_DATA / "original" / "output.csv",
        index_col="firms_id",
    )
    return results


@pytest.mark.parametrize("detection", detections)
def test_validate_known_points(validator, detection, expected_results):
    result: ValidationResult = validator.validate(detection, max_cloudy_percentage=10)

    expected = expected_results.loc[result.firms_id]

    assert result.burnt_pixel_count == expected.burnt_pixel_count
    assert result.burnt_building_count == expected.burnt_building_count
    assert result.burn_scar_detected == expected.burn_scar_detected
    assert result.no_data == expected.no_data
    assert result.too_cloudy == expected.too_cloudy
    assert str(result.before_date) == expected.before_date
    assert str(result.after_date) == expected.after_date
    assert str(result.acq_date) == expected.acq_date
