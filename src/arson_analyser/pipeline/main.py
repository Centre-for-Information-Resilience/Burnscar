import datetime
import logging
from pathlib import Path

from ..config import Config
from ..fetchers.gadm import ensure_gadm
from ..storage.duckdb import DuckDBStorage
from ..structure import check
from .sql import QueryLoader
from .steps.fetch import fetch_nasa_data
from .steps.validate import validate
from .steps.write import write

logger = logging.getLogger(__name__)


def run(
    config: Config,
    end_date: datetime.date,
):
    check(config)

    # collect missing FIRMS data
    fetch_nasa_data(
        config.api_key_nasa,
        config.paths.data,
        config.country_id,
        config.start_date,
        end_date,
    )

    # download GADM data
    path_gadm = ensure_gadm(
        config.paths.geo.gadm,
        config.country_id,
        config.gadm_level,
    )

    # setup storage
    storage = DuckDBStorage(config.paths.duckdb, ["spatial"])
    ql = QueryLoader(Path(__file__).parent / "queries")

    with storage:
        # create tables
        path_raw_data = str(config.paths.data / "*.parquet")
        create_tables_queries = [
            ql.load("create_gadm").params(
                gadm_path=str(path_gadm)
            ),  # administrative areas
            ql.load("create_gadm_index"),  # spatial index on gadm
            ql.load("create_settlements").params(
                path_settlements=str(config.paths.geo.settlements)
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
        storage.execute_all(create_tables_queries, silent=True)

        # ingest data
        inclusion_zones_query = ql.load(
            "insert_areas_include"
        )  # inclusion zone geometries
        inclusion_zones_queries = [
            inclusion_zones_query.params(path_areas_include=str(file))
            for file in config.paths.geo.include.glob("*.gpkg")
        ]  # we read all .gpkg files in the given directory

        exclusion_zones_query = ql.load("insert_areas_exclude")
        exclusion_zones_queries = [
            exclusion_zones_query.params(path_areas_exclude=str(file))
            for file in config.paths.geo.exclude.glob("*.gpkg")
        ]  # we read all .gpkg files in the given directory

        processing_queries = [
            ql.load("insert_raw_data").params(path_raw_data=path_raw_data),
            ql.load("insert_firms"),
            *inclusion_zones_queries,
            *exclusion_zones_queries,
        ]

        storage.execute_all(processing_queries)

        validate(
            storage=storage,
            ql=ql,
            ee_project=config.ee_project,
            max_cloudy_percentage=config.max_cloudy_percentage,
            retry_days=config.retry_days,
        )

        # cluster and join
        query = ql.load("create_clustered_events").params(
            max_date_gap=config.max_date_gap
        )
        storage.execute(query)

        # write to csv
        write(config, storage, ql)
