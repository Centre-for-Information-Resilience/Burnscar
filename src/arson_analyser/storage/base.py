from abc import ABC, abstractmethod

from pydantic import BaseModel


class BaseStorage(ABC):
    @abstractmethod
    def add(self, record: BaseModel):
        pass

    @abstractmethod
    def get(self, record_id: str) -> BaseModel:
        pass
