import datetime
import json
import logging
import time
from typing import Generator, Literal

import httpx
from pydantic import BaseModel

from .schema import NASARecord

logger = logging.getLogger(__name__)


def date_range(
    from_date: datetime.date, to_date: datetime.date, period: int = 10
) -> Generator[tuple[int, datetime.date], None, None]:
    # NASA uses [DATE] .. [DATE + DAY_RANGE-1] to fetch data
    days_in_range = (to_date - from_date).days

    for offset in range(days_in_range // period + 1):
        date = from_date + datetime.timedelta(days=offset * period)
        days_left = (to_date - date).days + 1

        if days_left < period:  # Last period
            yield (days_left, date)
        else:
            yield (period, date)


class RateLimits(BaseModel):
    api_key: str
    limit: int = 0
    used: int = 5000
    timeout: str = "10 minutes"

    @property
    def remaining(self) -> int:
        return self.limit - self.used

    def update(self):
        response = httpx.get(
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
    def __init__(
        self,
        api_key: str,
        instrument: Literal["VIIRS"] = "VIIRS",
        satellite: Literal["SNPP", "NOAA20", "NOAA21"] = "NOAA21",
        data_version: Literal["URT", "RT", "NRT", "SP"] = "NRT",
    ):
        self.api_key = api_key
        self.instrument = instrument
        self.satellite = satellite
        self.data_version = data_version
        self.base_url = "https://firms.modaps.eosdis.nasa.gov/api/country/csv"

        self.rate_limits = RateLimits(api_key=api_key)
        self.rate_limits.update()

        self.client = httpx.Client(timeout=60)

    def fetch(self, country_id: str, start_date: datetime.date, days: int) -> str:
        # wait for enough available transactions in our rate limit
        while self.rate_limits.remaining < 300:
            logger.info(
                f"Rate limit exceeded, waiting for 30 seconds. {self.rate_limits}"
            )
            time.sleep(30)
            self.rate_limits.update()

        logger.info(f"Fetching data for {country_id} from {start_date} for {days} days")

        url = (
            self.base_url
            + f"/{self.api_key}/{self.instrument}_{self.satellite}_{self.data_version}/{country_id}/{days}/{start_date}"
        )
        response = self.client.get(url)

        if not response.text.startswith("country_id,latitude,longitude"):
            raise ValueError("Invalid response: " + response.text)

        return response.text

    def parse(self, data: str) -> Generator[NASARecord, None, None]:
        lines = data.split("\n")
        header = lines[0].split(",")

        for record in lines[1:]:
            record_data = record.split(",")
            record_dict = dict(zip(header, record_data))
            yield NASARecord.model_validate(record_dict)

    def write(self, data: list[NASARecord], filename: str):
        with open(filename, "w") as file:
            json.dump(
                [record.model_dump(mode="json") for record in data], file, indent=2
            )

    def run(
        self,
        country_id: str,
        start_date: datetime.date,
        end_date: datetime.date,
        days: int,
    ) -> Generator[NASARecord, None, None]:
        for days, date in date_range(start_date, end_date, days):
            data = self.fetch(country_id, date, days)
            yield from self.parse(data)
            self.rate_limits.update()

    def run_and_write(
        self,
        country_id: str,
        start_date: datetime.date,
        end_date: datetime.date = datetime.date.today() + datetime.timedelta(days=1),
        days: int = 10,
        filename: str = "data.json",
    ):
        data = list(self.run(country_id, start_date, end_date, days))
        self.write(data, filename)
