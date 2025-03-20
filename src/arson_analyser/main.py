import datetime
import logging

from .config import Config
from .pipeline import collect
from .pipeline.sql import Query, QueryLoader
from .storage.duckdb import DuckDBStorage

logger = logging.getLogger(__name__)


def run(
    config: Config, country_id: str, start_date: datetime.date, end_date: datetime.date
):
    # collect
    collect.collect(
        config.api_key_nasa, config.path_data, country_id, start_date, end_date
    )

    # setup storage
    storage = DuckDBStorage(config.path_duckdb, ["spatial"])
    query_loader = QueryLoader(config.path_queries)

    raw_data_size = Query(name="raw_data_size", query="SELECT count(*) FROM raw_data")
    joined_size = Query(name="joined_size", query="SELECT count(*) FROM firms_joined")

    with storage:
        # create tables
        path_data_raw = str(config.path_data / "raw/*.parquet")
        query = query_loader.load_query("create_raw_data")
        query.parameters = {"path_raw_data": path_data_raw}
        storage.execute(query)

        query = query_loader.load_query("create_firms")
        storage.execute(query)

        query = query_loader.load_query("create_areas_whitelist")
        storage.execute(query)

        query = query_loader.load_query("create_firms_joined")
        storage.execute(query)

        # some stats
        logger.info(storage.execute(raw_data_size).show())
        # logger.info(storage.execute(filtered_size).show())

        # ingest
        query = query_loader.load_query("insert_raw_data")
        query.parameters = {"path_raw_data": path_data_raw}
        storage.execute(query)

        # process

        query = query_loader.load_query("insert_firms")
        storage.execute(query)

        # load whitelist

        query = query_loader.load_query("insert_areas_whitelist")
        query.parameters = {"path_areas_whitelist": str(config.path_areas_whitelist)}
        storage.execute(query)

        # spatial filter
        query = query_loader.load_query("insert_firms_joined")
        storage.execute(query)

        logger.info(storage.execute(raw_data_size).show())
        logger.info(storage.execute(joined_size).show())
        logger.info(storage.conn.sql("select * from firms_joined limit 5"))
        # cluster

        # analyse
