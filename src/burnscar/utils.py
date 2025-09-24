import datetime
import logging
import time
from typing import Any, Callable, Type, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


def retry(
    func, on: tuple[type[Exception]] = (Exception,), retries: int = 3, base: int = 2
):
    def wrapper(*args, **kwargs):
        for r in range(retries):
            try:
                return func(*args, **kwargs)
            except on as e:
                logger.error(f"Error on try {r}: {e}, waiting")
                time.sleep(base**r)

        return func(*args, **kwargs)  # final try

    return wrapper


def timeit(label: str):
    def decorator(func: Callable):
        def wrapper(*args, **kwargs):
            t0 = time.time()
            result = func(*args, **kwargs)
            print(f"{label:<40} took: {time.time() - t0:.2f}s")
            return result

        return wrapper

    return decorator


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
