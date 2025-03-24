from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__")

    api_key_nasa: str = ""
    path_data: Path = Path("data")
    path_duckdb: Path = Path("duckdb")
    path_queries: Path = Path(__file__).parent / "pipeline" / "queries"
    path_areas_include: Path = Path("geo/include")

    # Geo
    crs: str = "EPSG:4326"

    # google earth engine
    ee_project: str = "cir-arson-analyser"
