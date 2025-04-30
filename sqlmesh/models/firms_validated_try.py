import datetime
import typing as t

import pandas as pd

from arson_analyser.models import FireDetection
from arson_analyser.validators.gee import GEEValidator
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import ModelKindName


def validate(
    context: ExecutionContext, firms_to_validate: pd.DataFrame
) -> t.Generator[pd.DataFrame, None, None]:
    # set up validator
    ee_project = context.var("ee_project")
    assert ee_project, "GEE project not set in config"

    validator = GEEValidator(project=ee_project)

    # get validation params
    validation_params = context.var("validation_params", {})
    assert isinstance(validation_params, dict), (
        "Validation params should be a dictionary"
    )

    ee_concurrency = context.var("ee_concurrency")
    assert isinstance(ee_concurrency, int), (
        "Concurrency should be defined in the config and be a positive integer"
    )

    detections = [
        FireDetection.model_validate(d)
        for d in firms_to_validate.to_dict(orient="records")
    ]

    for validation_result in validator.validate_many(
        detections, validation_params=validation_params, max_workers=ee_concurrency
    ):
        validation_result_dict = validation_result.model_dump()
        yield pd.DataFrame([validation_result_dict])


@model(
    name="arson.firms_validated_try_1",
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        unique_key=("firms_id"),
        lookback="@analyse_until_days",
        batch_size=7,
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
def firms_validated_try_1(
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

    yield from validate(context, firms_to_validate)


@model(
    name="arson.firms_validated_try_2",
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        unique_key=("firms_id"),
        lookback="@analyse_until_days",
        batch_size=7,
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
def firms_validated_try_2(
    context: ExecutionContext,
    start: datetime.datetime,
    end: datetime.datetime,
    **kwargs: dict[str, t.Any],
) -> t.Generator[pd.DataFrame, None, None]:
    firms_to_validate_table = context.resolve_table("arson.firms_to_retry")

    firms_to_validate = context.fetchdf(
        f"""
        SELECT * FROM {firms_to_validate_table}
        WHERE acq_date BETWEEN '{start.date()}' AND '{end.date()}'
        """,
    )

    yield from validate(context, firms_to_validate)
