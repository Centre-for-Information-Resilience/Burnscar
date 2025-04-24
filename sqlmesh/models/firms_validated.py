import datetime
import typing as t

import pandas as pd

from arson_analyser.models import FireDetection
from arson_analyser.validators.gee import GEEValidator
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import ModelKindName


@model(
    name="arson.firms_validated",
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        unique_key=("firms_id"),
        lookback="@analyse_until_days",
        batch_size=1,
    ),
    columns={
        "firms_id": "int",
        "acq_date": "date",
        "burn_scar_detected": "bool",
        "burnt_pixel_count": "int",
        "burnt_building_count": "int",
        "no_data": "bool",
        "too_cloudy": "bool",
    },
)
def validate_firms(
    context: ExecutionContext,
    start: datetime.datetime,
    end: datetime.datetime,
    **kwargs: dict[str, t.Any],
) -> t.Generator[pd.DataFrame, None, None]:
    firms_to_validate_table = context.resolve_table("arson.firms_to_validate")
    firms_to_validate = context.fetchdf(
        f"""
        SELECT * FROM {firms_to_validate_table}
        WHERE acq_date BETWEEN '{start.date()}' AND '{end.date()}'
        """,
    )

    # set up validator
    ee_project = context.var("ee_project")
    assert ee_project, "GEE project not set in context"

    validator = GEEValidator(project=ee_project)

    # get validation params
    validation_params = context.var("validation_params", {})
    assert isinstance(validation_params, dict), (
        "Validation params should be a dictionary"
    )

    # validate each firms event
    for d in firms_to_validate.to_dict(orient="records"):
        detection = FireDetection.model_validate(d)
        validation_result = validator.validate(detection, **validation_params)
        validation_result_dict = validation_result.model_dump()

        yield pd.DataFrame([validation_result_dict])
