import datetime
import os
import typing as t

import pandas as pd
from dotenv import load_dotenv

from arson.models import FireDetection
from arson.validators.gee import GEEValidator
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import ModelKindName

COLUMNS = {
    "firms_id": "int",
    "acq_date": "date",
    "before_date": "date",
    "after_date": "date",
    "burn_scar_detected": "bool",
    "burnt_pixel_count": "int",
    "burnt_building_count": "int",
    "no_data": "bool",
    "too_cloudy": "bool",
}

KIND = dict(
    name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
    unique_key=("firms_id"),
    lookback="@validation_lookback",
    batch_size=7,
)


def validate(
    context: ExecutionContext,
    start: datetime.datetime,
    end: datetime.datetime,
    firms_to_validate_table: str,
) -> t.Generator[pd.DataFrame, None, None]:
    # fetch data
    firms_to_validate = context.fetchdf(
        f"""
        SELECT * FROM {firms_to_validate_table}
        WHERE acq_date BETWEEN '{start.date()}' AND '{end.date()}'
        """,
    )

    # set up validator
    load_dotenv()

    ee_project = os.getenv("EARTH_ENGINE_PROJECT_ID")
    assert ee_project, "GEE project id not set in .env file"

    ee_api_key = os.getenv("EARTH_ENGINE_API_KEY")
    assert ee_api_key, "GEE API key not set in .env file"

    validator = GEEValidator(project=ee_project, api_key=ee_api_key)

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
    name="arson.int_firms_validated_0",
    kind=KIND,
    columns=COLUMNS,
)
def firms_validated_try_0(
    context: ExecutionContext,
    start: datetime.datetime,
    end: datetime.datetime,
    **kwargs: dict[str, t.Any],
) -> t.Generator[pd.DataFrame, None, None]:
    firms_to_validate_table = context.resolve_table("arson.int_firms_to_validate_0")

    yield from validate(context, start, end, firms_to_validate_table)


@model(
    name="arson.int_firms_validated_1",
    kind=KIND,
    columns=COLUMNS,
)
def firms_validated_try_1(
    context: ExecutionContext,
    start: datetime.datetime,
    end: datetime.datetime,
    **kwargs: dict[str, t.Any],
) -> t.Generator[pd.DataFrame, None, None]:
    firms_to_validate_table = context.resolve_table("arson.int_firms_to_validate_1")

    yield from validate(context, start, end, firms_to_validate_table)


@model(
    name="arson.int_firms_validated_2",
    kind=KIND,
    columns=COLUMNS,
)
def firms_validated_try_2(
    context: ExecutionContext,
    start: datetime.datetime,
    end: datetime.datetime,
    **kwargs: dict[str, t.Any],
) -> t.Generator[pd.DataFrame, None, None]:
    firms_to_validate_table = context.resolve_table("arson.int_firms_to_validate_2")

    yield from validate(context, start, end, firms_to_validate_table)
