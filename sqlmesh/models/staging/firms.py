import datetime
import os
import typing as t

import pandas as pd
from burnscar.fetchers.nasa import NASAFetcher
from burnscar.utils import date_range
from dotenv import load_dotenv

from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import ModelKindName


@model(
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        time_column="acq_date",
        batch_size=1,
    ),
    cron="@daily",
    grain=("acq_date", "longitude", "latitude"),
    description="Raw NASA FIRMS data, fetched from the NASA API.",
    columns={
        "country_id": "text",
        "latitude": "double",
        "longitude": "double",
        "scan": "double",
        "track": "double",
        "acq_date": "date",
        "acq_time": "time",
        "satellite": "text",
        "instrument": "text",
        "version": "text",
        "frp": "double",
        "daynight": "text",
        "bright_ti4": "double",
        "bright_ti5": "double",
        "confidence": "text",
    },
)
def fetch_nasa_data(
    context: ExecutionContext,
    start: datetime.datetime,
    end: datetime.datetime,
    **kwargs: dict[str, t.Any],
) -> t.Generator[pd.DataFrame, None, None]:
    load_dotenv()
    api_key_nasa = os.getenv("NASA_API_KEY")
    assert api_key_nasa, "NASA API key not set in .env file"

    country_id = context.var("country_id")
    assert country_id, "Country code must be set in the context variables"

    fetcher = NASAFetcher(api_key=api_key_nasa)

    dfs = []

    # one fetch per date
    dates = date_range(start.date(), end.date())

    for date in dates:
        data = fetcher.fetch(country_id, date)
        dfs.append(fetcher.to_dataframe(data))

    # Concatenate all dataframes into one
    df = pd.concat(dfs, ignore_index=True)

    if df.empty:
        yield from ()

    else:
        yield df
