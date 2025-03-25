import datetime
import logging

from tqdm import tqdm

from .analyser.validate import FireDetection, FIRMSValidator
from .config import Config
from .fetchers.gadm import ensure_gadm
from .pipeline.collect import collect
from .pipeline.sql import QueryLoader
from .storage.duckdb import DuckDBStorage

logger = logging.getLogger(__name__)


def run(
    config: Config,
    country_id: str,
    gadm_level: int,
    start_date: datetime.date,
    end_date: datetime.date,
):
    # collect
    collect(config.api_key_nasa, config.path_data, country_id, start_date, end_date)

    # download GADM data
    path_gadm = ensure_gadm(config.path_gadm, country_id, gadm_level)

    # setup storage
    storage = DuckDBStorage(config.path_duckdb, ["spatial"])
    query_loader = QueryLoader(config.path_queries)

    with storage:
        # create tables

        ## administrative areas
        query = query_loader.load_query("create_gadm").with_parameters(
            gadm_path=str(path_gadm)
        )
        storage.execute(query)

        query = query_loader.load_query("create_gadm_index")
        storage.execute(query)

        query = query_loader.load_query("create_settlements").with_parameters(
            path_settlements=str(config.path_settlements)
        )
        storage.execute(query)

        query = query_loader.load_query("create_settlements_index")
        storage.execute(query)

        ## inclusion zones
        query = query_loader.load_query("create_areas_include")
        storage.execute(query)

        ## firms data
        path_raw_data = str(config.path_data / "raw/*.parquet")
        query = query_loader.load_query("create_raw_data").with_parameters(
            path_raw_data=path_raw_data
        )
        storage.execute(query)

        query = query_loader.load_query("create_firms")
        storage.execute(query)

        ## validation results table
        query = query_loader.load_query("create_validation_results")
        storage.execute(query)

        # ingest raw data
        query = query_loader.load_query("insert_raw_data").with_parameters(
            path_raw_data=path_raw_data
        )
        storage.execute(query)

        # process
        query = query_loader.load_query("insert_firms")
        storage.execute(query)

        # load inclusion zones
        query = query_loader.load_query("insert_areas_include")
        for file in config.path_areas_include.glob("*.gpkg"):
            storage.execute(query.with_parameters(path_areas_include=str(file)))

        # analyse firms detections
        query = query_loader.load_query("select_firms_analysis").with_parameters(
            retry_days=30
        )
        events = storage.execute(query)

        detections = [
            FireDetection(**dict(zip(events.columns, event)))
            for event in events.fetchall()
        ]

        query = query_loader.load_query("insert_validation_results")
        validator = FIRMSValidator(config.ee_project)
        for detection in tqdm(detections, "Analysing FIRMS detections"):
            result = validator.validate(detection)
            query = query.with_parameters(
                firms_id=result.firms_id,
                burn_scar_detected=result.burn_scar_detected,
                burnt_pixel_count=result.burnt_building_count,
                burnt_building_count=result.burnt_building_count,
                no_data=result.no_data,
                too_cloudy=result.too_cloudy,
            )
            storage.execute(query, silent=True)

        # cluster and join
        query = query_loader.load_query("create_clustered_events").with_parameters(
            max_date_gap=2
        )
        storage.execute(query)
        storage.conn.sql("select * from clustered_events").show()

        query = query_loader.load_query("select_gadm")
        gadm_df = storage.execute(query).df()

        query = query_loader.load_query("select_nearest_settlements")
        nearest_settlements_df = storage.execute(query).df()

        storage.conn.sql(
            f"""
            SELECT '{country_id}', 
                g.NAME_1, 
                g.NAME_2, 
                g.NAME_3, 
                s.settlement_name, 
                st_y(f.geom) AS latitude, 
                st_x(f.geom) AS longitude, 
                f.acq_date, 
                c.event_no, 
                c.area_include_id,
                v.no_data, 
                v.too_cloudy, 
                v.burn_scar_detected, 
                v.burnt_pixel_count, 
                v.burnt_building_count
            FROM validation_results v
                JOIN firms f
                    ON v.firms_id = f.id
                JOIN clustered_events c
                    ON f.area_include_id = c.area_include_id
                JOIN nearest_settlements_df s 
                    ON v.firms_id = s.firms_id
                JOIN gadm_df g 
                    ON v.firms_id = g.firms_id;
            """
        ).show()
