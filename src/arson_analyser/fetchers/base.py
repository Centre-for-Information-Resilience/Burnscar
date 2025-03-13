from abc import ABC, abstractmethod
from typing import Iterable

import httpx
from pydantic import BaseModel


class RateLimits(BaseModel):
    limit: int
    remaining: int
    reset: int


class BaseFetcher(ABC):
    rate_limits: RateLimits

    @abstractmethod
    def __init__(self, **kwargs):
        """Initialize the fetcher"""
        pass

    @abstractmethod
    def build_url(self, **kwargs) -> str:
        """Build URL for fetching data"""
        pass

    def fetch(self, url: str) -> str:
        """Fetch data from a source"""
        return httpx.get(url).text

    @abstractmethod
    def parse(
        self,
        data: str,
    ) -> Iterable[BaseModel]:
        """Parse the fetched data"""
        pass

    @abstractmethod
    def update_rate_limits(self):
        """Update rate limits"""
        pass
