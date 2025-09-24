import datetime
import os
import typing as t

import duckdb
import pandas as pd
from dotenv import load_dotenv
from sqlmesh.core.model import ModelKindName

from burnscar.fetchers.nasa import NASAFetcher
from burnscar.utils import date_range
from sqlmesh import ExecutionContext, model


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
    audits=[("number_of_rows", {"threshold": 1})],
)
def nasa_firms(
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

    gadm = context.resolve_table("reference.gadm")
    box = context.fetchdf(
        f"select st_extent(ST_Union_Agg(geom)) as box from {gadm}",
    )["box"][0]

    fetcher = NASAFetcher(api_key=api_key_nasa)

    dfs = []
    dates = date_range(start.date(), end.date())

    for date in dates:
        data = fetcher.fetch(box, date)
        dfs.append(fetcher.to_dataframe(data))

    df = pd.concat(dfs, ignore_index=True)

    df = duckdb.query(
        f"""
        load spatial;
        select '{country_id}' as country_id, *
        from df
        where st_within(st_point(longitude, latitude), (select ST_Union_Agg(geom) from {gadm}))
        """,
        connection=context.engine_adapter.connection,
    ).df()

    if df.empty:
        yield from ()

    else:
        yield df
