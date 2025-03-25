from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__")

    api_key_nasa: str = ""
    path_data: Path = Path("data")
    path_duckdb: Path = Path("duckdb")
    path_queries: Path = Path(__file__).parent / "pipeline" / "queries"

    # Geo data
    path_areas_include: Path = Path("geo/include")
    path_gadm: Path = Path("geo/gadm")
    path_settlements: Path = Path("geo/settlements/settlements.gpkg")

    # Geo
    crs: str = "EPSG:4326"

    # google earth engine
    ee_project: str = "cir-arson-analyser"
