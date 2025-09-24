import datetime
import json
import logging
import time
from enum import StrEnum
from pathlib import Path
from typing import Iterable, Literal, TypeVar

import httpx
import pandas as pd
from pydantic import BaseModel, field_validator

from ..utils import retry

T = TypeVar("T", bound=BaseModel)

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


class Satellite(StrEnum):
    SNPP = "N"
    NOAA20 = "N20"
    NOAA21 = "N21"


class Instrument(StrEnum):
    VIIRS = "VIIRS"


class Confidence(StrEnum):
    nominal = "n"
    low = "l"
    high = "h"


class DayNight(StrEnum):
    day = "D"
    night = "N"


class NASARecord(BaseModel):
    class Config:
        frozen = True

    latitude: float
    longitude: float
    scan: float
    track: float
    acq_date: datetime.date
    acq_time: datetime.time
    satellite: Satellite
    instrument: Instrument
    version: str
    frp: float
    daynight: DayNight
    bright_ti4: float
    bright_ti5: float
    confidence: Confidence

    @field_validator("acq_time", mode="before")
    def parse_time(cls, v):
        if isinstance(v, int):
            v_str = f"{v:04d}"  # Pad integer to ensure four digits
        elif isinstance(v, str):
            v_str = v.zfill(4)  # Handle strings, pad left zeros
        else:
            raise ValueError("Invalid time format")

        hour = int(v_str[:2])
        minute = int(v_str[2:])

        return datetime.time(hour=hour, minute=minute)

    def __hash__(self):
        return hash(
            (
                self.latitude,
                self.longitude,
                self.acq_date,
                self.acq_time,
            )
        )


class RateLimits:
    def __init__(
        self,
        client: httpx.Client,
        api_key: str,
        timeout: str = "10 minutes",
    ):
        self.client = client
        self.api_key = api_key
        self.timeout = timeout

        self.limit = 0
        self.used = 5000

    @property
    def remaining(self) -> int:
        return self.limit - self.used

    def update(self):
        response = self.client.get(
            "https://firms.modaps.eosdis.nasa.gov/mapserver/mapkey_status/?MAP_KEY="
            + self.api_key
        )
        response_data = response.json()

        self.limit = response_data["transaction_limit"]
        self.used = response_data["current_transactions"]
        self.timeout = response_data["transaction_interval"]

        logger.debug(f"Rate limits: {self}")

    def __str__(self):
        return f"{self.used}/{self.limit} ({self.timeout})"


class NASAFetcher:
    satellites = ("SNPP", "NOAA20", "NOAA21")
    base_url = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

    def __init__(
        self,
        api_key: str,
        instrument: Literal["VIIRS"] = "VIIRS",
        data_version: Literal["URT", "RT", "NRT", "SP"] = "NRT",
    ):
        self.api_key = api_key
        self.instrument = instrument
        self.data_version = data_version

        self.client = httpx.Client(timeout=60)
        self.rate_limits = RateLimits(api_key=api_key, client=self.client)

    @retry
    def _fetch_raw(
        self,
        box: dict[str, float],
        date: datetime.date,
        satellite: str,
    ) -> str:
        # wait for enough available transactions in our rate limit
        # NASA uses some sort of rolling window for rate limits
        self.rate_limits.update()
        while self.rate_limits.remaining < 30:
            logger.debug(
                f"Rate limit exceeded, waiting for 10 seconds. {self.rate_limits}"
            )
            time.sleep(10)
            self.rate_limits.update()

        logger.debug(f"Fetching data for {box} on {date}")

        area = "{min_x},{min_y},{max_x},{max_y}".format(**box)

        url = (
            self.base_url
            + f"/{self.api_key}/{self.instrument}_{satellite}_{self.data_version}/{area}/1/{date}"
        )
        response = self.client.get(url)

        if not response.text.startswith("latitude,longitude"):
            raise ValueError("Invalid response: " + response.text)

        return response.text

    def parse(self, data: str) -> list[NASARecord]:
        lines = data.splitlines()
        header = lines[0].split(",")

        parsed_data = []
        for record in lines[1:]:
            record_data = record.split(",")
            record_dict = dict(zip(header, record_data))
            parsed_data.append(NASARecord.model_validate(record_dict))

        return parsed_data

    def fetch(
        self,
        box: dict[str, float],
        date: datetime.date,
        satellites: Iterable[str] | None = None,
    ) -> list[NASARecord]:
        # We are iterating over the satellites because they are one constellation
        # and the data they output is in the same format. This way we can partion
        # by just country and date.
        parsed_data = []
        for satellite in satellites or self.satellites:
            data = self._fetch_raw(box, date, satellite)
            parsed_data += self.parse(data)

        return parsed_data

    @staticmethod
    def serialize(data: list[T]) -> list[dict]:
        return [record.model_dump() for record in data]

    @staticmethod
    def deserialize(data: list[dict], model: T) -> list[T]:
        return [model.model_validate(record) for record in data]

    @staticmethod
    def to_dataframe(data: list[T]) -> pd.DataFrame:
        return pd.DataFrame([record.model_dump() for record in data])

    def to_parquet(self, data: list[T], path: Path):
        df = self.to_dataframe(data)
        df.to_parquet(path, index=False)

    @staticmethod
    def to_json(data: list[T], path: Path):
        with open(path, "w") as f:
            json.dump([record.model_dump(mode="json") for record in data], f, indent=2)
