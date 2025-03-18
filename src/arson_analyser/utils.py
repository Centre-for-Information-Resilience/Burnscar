import datetime
import logging
import time
from typing import Iterable

logger = logging.getLogger(__name__)


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
) -> Iterable[datetime.date]:
    return (
        start_date + datetime.timedelta(days=i)
        for i in range((end_date - start_date).days + 1)
    )
