import datetime
import logging

from arson_analyser.config import Config
from arson_analyser.pipeline.main import run
from arson_analyser.storage.duckdb import DuckDBStorage

logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    config = Config()
    storage = DuckDBStorage(config.paths.duckdb, extensions=["spatial"])

    run(
        config,
        datetime.date(2025, 1, 30),
    )
