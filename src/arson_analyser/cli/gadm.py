import typer

from ..config import Config
from ..fetchers.gadm import ensure_gadm

app = typer.Typer()
config = Config()


@app.command()
def fetch(country_id: str = typer.Option("SDN"), level: int = typer.Option(3)):
    path = ensure_gadm(path=config.paths.geo.gadm, country_id=country_id, level=level)
    typer.echo(path)


if __name__ == "__main__":
    app()
