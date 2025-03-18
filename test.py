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
        datetime.date(2025, 2, 1),
    )

    storage = DuckDBStorage(config.path_duckdb, extensions=["spatial"])

    with storage:
        storage.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS raw_data AS 
            SELECT * FROM read_parquet('{config.path_data}/raw/*.parquet', filename = false)
            LIMIT 0
            """,
        )

        storage.conn.execute(
            f"""
            INSERT INTO raw_data
            SELECT * FROM read_parquet('{config.path_data}/raw/*.parquet', filename = false)
            WHERE acq_date NOT IN (SELECT DISTINCT acq_date FROM raw_data);
            """,
        )

        storage.conn.execute(
            """CREATE OR REPLACE TABLE geom_data AS
                (SELECT acq_date, ST_POINT(longitude, latitude) AS geom FROM raw_data)"""
        )

        print(storage.conn.sql("SELECT COUNT(*) FROM geom_data"))

        log.create_log_table(storage)
        log.insert_log(storage, datetime.date.today(), log.Step.filtering)

        log.update_log(
            storage, datetime.date.today(), log.Step.filtering, log.Status.success, True
        )

        storage.conn.execute(
            """
            CREATE OR REPLACE TABLE urban_areas AS
            SELECT unnest(st_dump(geom)).geom as geom FROM ST_Read('geo/urban_areas.gpkg');"""
        )

        print(storage.conn.sql("select * from urban_areas limit 5;"))

        print("Creating indices...")
        storage.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS urban_areas_idx ON urban_areas USING RTREE (geom);
            """
        )

        print("Intersecting...")
        storage.conn.execute(
            """
            CREATE OR REPLACE TABLE urban_filtered AS
            SELECT g.*
            FROM geom_data AS g
            JOIN urban_areas AS u ON st_within(g.geom, u.geom)
            """
        )

        print(storage.conn.sql("SELECT COUNT(*) FROM urban_filtered"))
