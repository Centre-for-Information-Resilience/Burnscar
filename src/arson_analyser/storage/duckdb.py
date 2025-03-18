import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


class DuckDBStorage:
    def __init__(self, path: Path, extensions: list[str]):
        self.path = path / "data.duckdb"
        self.extensions = extensions

    def _install_extensions(self):
        for ext in self.extensions:
            self.conn.sql(f"INSTALL {ext}; LOAD {ext};")

    def __enter__(self):
        self.conn = duckdb.connect(self.path)

        self._install_extensions()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
