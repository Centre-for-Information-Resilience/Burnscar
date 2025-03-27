import datetime

import typer

from ..config import Config
from ..main import run

app = typer.Typer()
config = Config()


@app.command()
def full(
    country_id: str = typer.Option("SDN"),
    gadm_level: int = typer.Option(3),
    start_date: datetime.date = typer.Option(...),
    end_date: datetime.date = typer.Option(
        datetime.date.today() - datetime.timedelta(days=5)
    ),
):
    run(
        config=config,
        country_id=country_id,
        gadm_level=gadm_level,
        start_date=start_date,
        end_date=end_date,
    )


if __name__ == "__main__":
    app()
