import datetime

import typer

from ..config import Config
from ..pipeline.main import run

app = typer.Typer()
config = Config()


@app.command()
def full(
    end_date: str = typer.Option(str(datetime.date.today())),
):
    latest_end_date = datetime.date.today() - datetime.timedelta(
        days=config.max_date_gap
    )
    if datetime.date.fromisoformat(end_date) > latest_end_date:
        end_date = str(latest_end_date)

    typer.echo(f"Running for {config.country_id}, {config.start_date} - {end_date}")

    run(
        config=config,
        end_date=datetime.date.fromisoformat(end_date),
    )


if __name__ == "__main__":
    app()
