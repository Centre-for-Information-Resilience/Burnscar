from ...models import FireDetection
from ...storage.base import BaseStorage
from ...utils import progress
from ...validators.gee import GEEValidator
from ..sql import QueryLoader


def validate(
    storage: BaseStorage,
    ql: QueryLoader,
    ee_project: str,
    max_cloudy_percentage: int,
    retry_days: int,
) -> None:
    # select data for validation
    events = storage.execute(
        ql.load("select_firms_validation").params(retry_days=retry_days)
    )

    detections = [
        FireDetection(**dict(zip(events.columns, event))) for event in events.fetchall()
    ]

    # GEE validation starts here
    validator = GEEValidator(ee_project)
    insert_results_query = ql.load("insert_validation_results")
    with progress:
        for detection in progress.track(detections, description="Analysing..."):
            result = validator.validate(
                detection, max_cloudy_percentage=max_cloudy_percentage
            )
            query = insert_results_query.params(
                firms_id=result.firms_id,
                burn_scar_detected=result.burn_scar_detected,
                burnt_pixel_count=result.burnt_pixel_count,
                burnt_building_count=result.burnt_building_count,
                no_data=result.no_data,
                too_cloudy=result.too_cloudy,
            )
            storage.execute(
                query, silent=True
            )  # insert every detection early because this is costly
