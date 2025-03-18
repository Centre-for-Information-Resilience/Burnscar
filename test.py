import datetime
import logging

from arson_analyser.config import Config
from arson_analyser.pipeline import log
from arson_analyser.pipeline.collect import collect
from arson_analyser.storage.duckdb import DuckDBStorage

logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    config = Config()

    collect(
        config.api_key_nasa,
        config.path_data,
        "SDN",
        datetime.date(2025, 2, 1),
        datetime.date(2025, 2, 28),
    )

    storage = DuckDBStorage(config.path_duckdb, extensions=["spatial"])

    with storage:
        storage.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS nasa_viirs AS 
            SELECT * FROM read_parquet('{config.path_data}/raw/*.parquet', filename = false)
            LIMIT 0
            """,
        )

        log.create_log_table(storage)
        log.insert_log(storage, datetime.date.today(), log.Step.filtering)

        print(storage.conn.sql("SHOW ALL TABLES"))
        print(storage.conn.sql("SELECT * FROM processing_log"))

        log.update_log(
            storage, datetime.date.today(), log.Step.filtering, log.Status.success, True
        )

        print(storage.conn.sql("SELECT * FROM processing_log"))
