import datetime
import logging
import time
from typing import Any, Type, TypeVar

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

progress = Progress(
    SpinnerColumn(),
    TimeElapsedColumn(),
    TextColumn("[progress.description]{task.description}"),
    BarColumn(),
    MofNCompleteColumn(),
    TimeRemainingColumn(),
)


def retry(func, retries: int = 3, base: int = 2):
    def wrapper(*args, **kwargs):
        for r in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error on try {r}: {e}, waiting")
                time.sleep(base**r)
        raise ValueError("Failed after retries")

    return wrapper


def date_range(
    start_date: datetime.date, end_date: datetime.date
) -> list[datetime.date]:
    return [
        start_date + datetime.timedelta(days=i)
        for i in range((end_date - start_date).days + 1)
    ]


def expect_type(obj: Any, expected_type: Type[T], default: T) -> T:
    if not isinstance(obj, expected_type):
        logger.warning(
            f"Expected type {expected_type.__name__}, but got {type(obj).__name__}. Using default: {default!r}"
        )
        return default
    return obj
