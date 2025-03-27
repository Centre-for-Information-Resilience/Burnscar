import typer

from . import fetch, gadm, run, validate

app = typer.Typer()

app.add_typer(fetch.app, name="fetch")
app.add_typer(gadm.app, name="gadm")
app.add_typer(validate.app, name="validate")
app.add_typer(run.app, name="run")

if __name__ == "__main__":
    app()
