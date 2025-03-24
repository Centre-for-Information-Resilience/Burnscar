import datetime
import logging

from .analyser.validate import FireDetection, FIRMSValidator
from .config import Config
from .pipeline.collect import collect
from .pipeline.sql import Query, QueryLoader
from .storage.duckdb import DuckDBStorage

logger = logging.getLogger(__name__)


def run(
    config: Config, country_id: str, start_date: datetime.date, end_date: datetime.date
):
    # collect
    collect(config.api_key_nasa, config.path_data, country_id, start_date, end_date)

    # setup storage
    storage = DuckDBStorage(config.path_duckdb, ["spatial"])
    query_loader = QueryLoader(config.path_queries)

    raw_data_size = Query(name="raw_data_size", query="SELECT count(*) FROM raw_data")
    joined_size = Query(name="joined_size", query="SELECT count(*) FROM firms_joined")

    with storage:
        # create tables
        path_raw_data = str(config.path_data / "raw/*.parquet")
        query = query_loader.load_query("create_raw_data").with_parameters(
            path_raw_data=path_raw_data
        )
        storage.execute(query)

        query = query_loader.load_query("create_firms")
        storage.execute(query)

        query = query_loader.load_query("create_areas_include")
        storage.execute(query)

        query = query_loader.load_query("create_firms_joined")
        storage.execute(query)

        # ingest
        query = query_loader.load_query("insert_raw_data").with_parameters(
            path_raw_data=path_raw_data
        )
        storage.execute(query)

        # process

        query = query_loader.load_query("insert_firms")
        storage.execute(query)

        # load includes
        query = query_loader.load_query("insert_areas_include")
        for file in config.path_areas_include.glob("*.gpkg"):
            storage.execute(query.with_parameters(path_areas_include=str(file)))

        # spatial filter
        query = query_loader.load_query("insert_firms_joined")
        storage.execute(query)

        logger.info(storage.execute(raw_data_size).show())
        logger.info(storage.execute(joined_size).show())
        logger.info(storage.conn.sql("select * from firms_joined limit 5"))
        logger.info(storage.conn.sql("select count(*) from areas_include"))

        # cluster
        query = query_loader.load_query("merge_events").with_parameters(max_date_gap=2)
        events = storage.execute(query)
        logger.info(events.show())

        # analyse
        validator = FIRMSValidator(config.ee_project)
        query = query_loader.load_query("select_firms_analysis")
        events = storage.execute(query)
        for event in events.fetchmany(10):
            data = dict(zip(events.columns, event))
            detection = FireDetection(**data)
            logger.info(f"Validating: {detection}")
            validator.validate(detection)
