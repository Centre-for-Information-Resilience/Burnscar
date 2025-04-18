import datetime
from pathlib import Path

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class GeoPaths(BaseModel):
    base: Path = Path("geo")
    include: Path = Path("include")
    exclude: Path = Path("exclude")
    gadm: Path = Path("gadm")
    settlements: Path = Path("settlements.gpkg")


class Paths(BaseModel):
    data: Path = Path("data")
    duckdb: Path = Path("duckdb")
    output: Path = Path("output")

    geo: GeoPaths = GeoPaths()

    @model_validator(mode="after")
    def set_subdirs(self) -> "Paths":
        def resolve_path(base: Path, sub: Path) -> Path:
            if sub.is_absolute() or sub.parts[: len(base.parts)] == base.parts:
                return sub
            return base / sub

        self.geo.include = resolve_path(self.geo.base, self.geo.include)
        self.geo.exclude = resolve_path(self.geo.base, self.geo.exclude)
        self.geo.gadm = resolve_path(self.geo.base, self.geo.gadm)
        self.geo.settlements = resolve_path(self.geo.base, self.geo.settlements)
        return self


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_nested_delimiter="__")

    # Credentials
    api_key_nasa: str = Field(..., min_length=32, max_length=32)

    # google earth engine
    ee_project: str = "cir-arson-analyser"

    # Geo
    crs: str = "EPSG:4326"

    # paths
    paths: Paths = Paths()

    # Project settings
    country_id: str = Field(..., min_length=3, max_length=3)
    gadm_level: int = Field(3, ge=1, le=3)
    start_date: datetime.date = Field(datetime.date(2025, 1, 1))

    # Analysis settings
    max_cloudy_percentage: int = 20
    retry_days: int = 30
    max_date_gap: int = 2

    @field_validator("start_date", mode="before")
    def parse_dates(cls, v: str) -> datetime.date:
        return datetime.date.fromisoformat(v)
