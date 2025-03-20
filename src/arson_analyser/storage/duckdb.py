import logging
from pathlib import Path

import duckdb

from ..pipeline.sql import Query

logger = logging.getLogger(__name__)


class DuckDBStorage:
    def __init__(self, path: Path, extensions: list[str]):
        self.path = path / "data.duckdb"
        self.extensions = extensions

    def _install_extensions(self):
        for ext in self.extensions:
            self.conn.install_extension(ext)
            self.conn.load_extension(ext)

    def __enter__(self):
        self.conn = duckdb.connect(self.path)
        self.conn.execute("SET enable_progress_bar = true;")

        self._install_extensions()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def execute(self, query: Query):
        logger.info(f"Executing: {query}")
        assert all(k in query.parameters for k in query.parameter_names), (
            f"not all parameters present: {query.parameter_names}"
        )
        return self.conn.sql(query.query, params=query.parameters)
