import datetime

import typer

from ..config import Config
from ..pipeline.steps.fetch import fetch_nasa_data

app = typer.Typer()
config = Config()


@app.command()
def fetch(
    country_id: str = typer.Option("SDN"),
    end_date: str = typer.Option(datetime.date.today()),
):
    fetch_nasa_data(
        api_key=config.api_key_nasa,
        data_path=config.paths.data,
        country_id=country_id,
        start_date=config.start_date,
        end_date=datetime.date.fromisoformat(end_date),
    )


if __name__ == "__main__":
    app()
