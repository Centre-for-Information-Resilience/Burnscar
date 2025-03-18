from pathlib import Path

from ..storage.duckdb import DuckDBStorage


def ingest(storage: DuckDBStorage, data_path: Path):
    with storage:
        storage.ingest_parquet(data_path)
