import datetime
import logging

import pandas as pd
from tqdm import tqdm

from .config import Config
from .fetchers.gadm import ensure_gadm
from .linkgen import copernicus, whopostedwhat, x
from .pipeline.fetch import fetch_nasa_data
from .pipeline.sql import QueryLoader
from .storage.duckdb import DuckDBStorage
from .validators.gee import GEEValidator
from .validators.models import FIRMSDetection

logger = logging.getLogger(__name__)


def run(
    config: Config,
    country_id: str,
    gadm_level: int,
    start_date: datetime.date,
    end_date: datetime.date,
    retry_days: int = 30,
    max_cloudy_percentage: int = 20,
    max_date_gap: int = 2,
):
    # collect missing FIRMS data
    fetch_nasa_data(
        config.api_key_nasa, config.path_data, country_id, start_date, end_date
    )

    # download GADM data
    path_gadm = ensure_gadm(config.path_gadm, country_id, gadm_level)

    # setup storage
    storage = DuckDBStorage(config.path_duckdb, ["spatial"])
    ql = QueryLoader(config.path_queries)

    path_raw_data = str(config.path_data / "raw/*.parquet")

    create_tables_queries = [
        ql.load("create_gadm").params(gadm_path=str(path_gadm)),  # administrative areas
        ql.load("create_gadm_index"),  # spatial index on gadm
        ql.load("create_settlements").params(
            path_settlements=str(config.path_settlements)
        ),  # settlement names
        ql.load("create_settlements_index"),  # spatial index on settlements
        ql.load("create_areas_include"),  # inclusion zones
        ql.load("create_areas_exclude"),  # exclusion zones
        ql.load("create_raw_data").params(
            path_raw_data=path_raw_data
        ),  # raw firms data
        ql.load("create_firms"),  # firms spatially joined to inclusion zones
        ql.load("create_validation_results"),  # table for validation results
        ql.load("create_view_nearest_settlements"),  # view for nearest settlements
    ]

    inclusion_zones_query = ql.load("insert_areas_include")  # inclusion zone geometries
    inclusion_zones_queries = [
        inclusion_zones_query.params(path_areas_include=str(file))
        for file in config.path_areas_include.glob("*.gpkg")
    ]  # we read all .gpkg files in the given directory

    exclusion_zones_query = ql.load("insert_areas_exclude")
    exclusion_zones_queries = [
        exclusion_zones_query.params(path_areas_exclude=str(file))
        for file in config.path_areas_exclude.glob("*.gpkg")
    ]  # we read all .gpkg files in the given directory

    processing_queries = [
        ql.load("insert_raw_data").params(path_raw_data=path_raw_data),
        ql.load("insert_firms"),
        *inclusion_zones_queries,
        *exclusion_zones_queries,
    ]

    with storage:
        # create tables
        storage.execute_all(create_tables_queries, silent=True)

        # ingest data
        storage.execute_all(processing_queries)

        # select data for validation
        events = storage.execute(
            ql.load("select_firms_validation").params(retry_days=retry_days)
        )

        detections = [
            FIRMSDetection(**dict(zip(events.columns, event)))
            for event in events.fetchall()
        ]

        # GEE validation starts here
        validator = GEEValidator(config.ee_project)

        insert_results_query = ql.load("insert_validation_results")
        for detection in tqdm(detections, "Analysing FIRMS detections"):
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

        # cluster and join
        query = ql.load("create_clustered_events").params(max_date_gap=max_date_gap)
        storage.execute(query)

        # write to csv
        config.path_output.mkdir(exist_ok=True)
        query = ql.load("select_output")
        output = storage.execute(query).df()
        output = add_links(output)

        output.to_csv(config.path_output / "validated_detections.csv")

        storage.conn.sql("select * from clustered_events;").to_csv(
            str(config.path_output / "clustered_events.csv")
        )


def add_links(
    output: pd.DataFrame,
    date_window_size=5,
    keyword_cols=["settlement_name", "NAME_1", "NAME_2", "NAME_3"],
) -> pd.DataFrame:
    rows = []
    for _, row in output.iterrows():
        start_date = (
            row["acq_date"] - datetime.timedelta(days=date_window_size)
        ).date()
        end_date = (row["acq_date"] + datetime.timedelta(days=date_window_size)).date()

        links = {
            "firms_id": row["firms_id"],
        }

        links["link_copernicus"] = copernicus(
            row["latitude"], row["longitude"], start_date, end_date
        )

        for keyword_col in keyword_cols:
            links[f"link_{keyword_col}_x"] = x(row[keyword_col], start_date, end_date)
            links[f"link_{keyword_col}_whopostedwhat"] = whopostedwhat(
                row[keyword_col], start_date, end_date
            )

        rows.append(links)

    links_df = pd.DataFrame.from_records(rows).set_index("firms_id")
    output = output.join(links_df)
    return output
