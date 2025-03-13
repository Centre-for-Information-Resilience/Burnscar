import datetime
import logging

from arson_analyser.config import Config
from arson_analyser.fetchers.nasa.fetcher import NASAFetcher

logging.basicConfig(level=logging.INFO)

config = Config()

fetcher = NASAFetcher(config.nasa_api_key)
fetcher.run_and_write("SDN", datetime.date(2024, 1, 1), datetime.date.today())
