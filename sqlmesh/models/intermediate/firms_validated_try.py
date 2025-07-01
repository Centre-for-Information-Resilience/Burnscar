import datetime
import typing as t
from pathlib import Path

import pandas as pd
from burnscar.models import FireDetection
from burnscar.validators.gee import GEEValidator
from dotenv import load_dotenv

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
    "validation_try": "int",
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
    try_: int = 0,
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
    ee_key_path = context.var("ee_key_path")
    assert ee_key_path, "ee_key_path must be set in config"
    ee_key_path = Path(ee_key_path)
    assert ee_key_path.exists(), f"Earth Engine key file is missing: {ee_key_path}"

    validator = GEEValidator(key_path=ee_key_path)

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
        result_df = pd.DataFrame([validation_result_dict])
        result_df["validation_try"] = try_

        yield result_df


@model(
    name="intermediate.firms_validated_0",
    kind=KIND,
    columns=COLUMNS,
)
def firms_validated_try_0(
    context: ExecutionContext,
    start: datetime.datetime,
    end: datetime.datetime,
    **kwargs: dict[str, t.Any],
) -> t.Generator[pd.DataFrame, None, None]:
    firms_to_validate_table = context.resolve_table("intermediate.firms_to_validate_0")

    yield from validate(
        context=context,
        start=start,
        end=end,
        firms_to_validate_table=firms_to_validate_table,
        try_=0,
    )


@model(
    name="intermediate.firms_validated_1",
    kind=KIND,
    columns=COLUMNS,
)
def firms_validated_try_1(
    context: ExecutionContext,
    start: datetime.datetime,
    end: datetime.datetime,
    **kwargs: dict[str, t.Any],
) -> t.Generator[pd.DataFrame, None, None]:
    firms_to_validate_table = context.resolve_table("intermediate.firms_to_validate_1")

    yield from validate(
        context=context,
        start=start,
        end=end,
        firms_to_validate_table=firms_to_validate_table,
        try_=1,
    )


@model(
    name="intermediate.firms_validated_2",
    kind=KIND,
    columns=COLUMNS,
)
def firms_validated_try_2(
    context: ExecutionContext,
    start: datetime.datetime,
    end: datetime.datetime,
    **kwargs: dict[str, t.Any],
) -> t.Generator[pd.DataFrame, None, None]:
    firms_to_validate_table = context.resolve_table("intermediate.firms_to_validate_2")

    yield from validate(
        context=context,
        start=start,
        end=end,
        firms_to_validate_table=firms_to_validate_table,
        try_=2,
    )
