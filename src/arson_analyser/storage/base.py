import logging
from abc import ABC, abstractmethod
from pathlib import Path

from ..pipeline.sql import Query

logger = logging.getLogger(__name__)


class BaseStorage(ABC):
    @abstractmethod
    def __init__(self, path: Path, extensions: list[str]):
        pass

    @abstractmethod
    def _install_extensions(self):
        pass

    @abstractmethod
    def __enter__(self):
        self._install_extensions()
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def execute(self, query: Query, silent: bool = False):
        pass

    def execute_all(self, queries: list[Query], silent=False):
        for query in queries:
            self.execute(query, silent)
