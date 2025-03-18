import datetime
import json
import logging
import time
from pathlib import Path
from typing import Iterable, Literal, TypeVar

import httpx
import pandas as pd
from pydantic import BaseModel

from ...utils import retry
from .schema import NASARecord

T = TypeVar("T", bound=BaseModel)

logger = logging.getLogger(__name__)

client = httpx.Client(timeout=60)


class RateLimits(BaseModel):
    api_key: str
    limit: int = 0
    used: int = 5000
    timeout: str = "10 minutes"

    @property
    def remaining(self) -> int:
        return self.limit - self.used

    def update(self):
        response = client.get(
            "https://firms.modaps.eosdis.nasa.gov/mapserver/mapkey_status/?MAP_KEY="
            + self.api_key
        )
        response_data = response.json()

        self.limit = response_data["transaction_limit"]
        self.used = response_data["current_transactions"]
        self.timeout = response_data["transaction_interval"]

        logger.info(f"Rate limits: {self}")

    def __str__(self):
        return f"{self.used}/{self.limit} ({self.timeout})"


class NASAFetcher:
    satellites = ("SNPP", "NOAA20", "NOAA21")
    base_url = "https://firms.modaps.eosdis.nasa.gov/api/country/csv"

    def __init__(
        self,
        api_key: str,
        instrument: Literal["VIIRS"] = "VIIRS",
        data_version: Literal["URT", "RT", "NRT", "SP"] = "NRT",
        data_path: Path = Path("data"),
    ):
        self.api_key = api_key
        self.instrument = instrument
        self.data_version = data_version
        self.data_path = data_path
        self.out_path = self.data_path / "raw"

        self.rate_limits = RateLimits(api_key=api_key)
        self.rate_limits.update()

    @retry
    def _fetch_raw(self, country_id: str, date: datetime.date, satellite: str) -> str:
        # wait for enough available transactions in our rate limit
        # NASA uses some sort of rolling window for rate limits
        while self.rate_limits.remaining < 30:
            logger.info(
                f"Rate limit exceeded, waiting for 10 seconds. {self.rate_limits}"
            )
            time.sleep(10)
            self.rate_limits.update()

        logger.info(f"Fetching data for {country_id} on {date}")

        url = (
            self.base_url
            + f"/{self.api_key}/{self.instrument}_{satellite}_{self.data_version}/{country_id}/1/{date}"
        )
        response = client.get(url)

        if not response.text.startswith("country_id,latitude,longitude"):
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
        country_id: str,
        date: datetime.date,
        satellites: Iterable[str] | None = None,
    ) -> list[NASARecord]:
        # We are iterating over the satellites because they are one constellation
        # and the data they output is in the same format. This way we can partion
        # by just country and date.
        parsed_data = []
        for satellite in satellites or self.satellites:
            data = self._fetch_raw(country_id, date, satellite)
            self.rate_limits.update()
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
