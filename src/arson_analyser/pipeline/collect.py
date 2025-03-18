import datetime
import logging
from pathlib import Path

from ..fetchers.nasa.fetcher import NASAFetcher
from ..utils import date_range

logger = logging.getLogger(__name__)


def collect(
    api_key: str,
    data_path: Path,
    country_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
    overwrite: bool = False,
):
    fetcher = NASAFetcher(api_key=api_key, data_path=data_path)
    dates = date_range(start_date, end_date)

    for date in dates:
        if not overwrite and (fetcher.out_path / f"{date}.parquet").exists():
            logger.info(f"Skipping {date}")
            continue

        logger.info(f"Fetching {date}")
        data = fetcher.fetch(country_id, date)
        fetcher.out_path.mkdir(parents=True, exist_ok=True)
        fetcher.to_parquet(data, fetcher.out_path / f"{date}.parquet")

    logger.info("Done")
