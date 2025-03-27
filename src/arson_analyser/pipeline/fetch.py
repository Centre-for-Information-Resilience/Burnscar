import datetime
from pathlib import Path

from tqdm import tqdm

from ..fetchers.nasa.fetcher import NASAFetcher
from ..utils import date_range


def fetch_nasa_data(
    api_key: str,
    data_path: Path,
    country_id: str,
    start_date: datetime.date,
    end_date: datetime.date,
    overwrite: bool = False,
):
    fetcher = NASAFetcher(api_key=api_key, data_path=data_path)
    dates = date_range(start_date, end_date)

    dates = [
        date
        for date in dates
        if overwrite or not (fetcher.out_path / f"{date}.parquet").exists()
    ]

    for date in tqdm(
        dates,
        "Fetching FIRMS data",
    ):
        data = fetcher.fetch(country_id, date)
        fetcher.out_path.mkdir(parents=True, exist_ok=True)
        fetcher.to_parquet(data, fetcher.out_path / f"{date}.parquet")
