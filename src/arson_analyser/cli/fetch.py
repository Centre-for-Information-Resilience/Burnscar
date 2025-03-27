import datetime

import typer

from ..config import Config
from ..pipeline.fetch import fetch_nasa_data

app = typer.Typer()
config = Config()


@app.command()
def fetch(
    country_id: str = typer.Option("SDN"),
    start_date: datetime.date = typer.Option(...),
    end_date: datetime.date = typer.Option(...),
):
    fetch_nasa_data(
        api_key=config.api_key_nasa,
        data_path=config.path_data,
        country_id=country_id,
        start_date=start_date,
        end_date=end_date,
    )


if __name__ == "__main__":
    app()
