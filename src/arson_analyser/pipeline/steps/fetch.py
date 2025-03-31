import datetime
from pathlib import Path

from ...fetchers.nasa.fetcher import NASAFetcher
from ...utils import date_range, progress


def fetch_nasa_data(
    api_key: str,
    data_path: Path,
    country_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
    overwrite: bool = False,
) -> None:
    fetcher = NASAFetcher(api_key=api_key, data_path=data_path)
    dates = date_range(start_date, end_date)

    dates = [
        date
        for date in dates
        if overwrite or not (fetcher.out_path / f"{date}.parquet").exists()
    ]

    with progress:
        for date in progress.track(dates, description="Fetching..."):
            data = fetcher.fetch(country_id, date)
            fetcher.to_parquet(data, fetcher.out_path / f"{date}.parquet")
