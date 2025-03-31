import typer

from ..config import Config
from ..structure import collect_paths, get_missing_paths

app = typer.Typer()
config = Config()


@app.command()
def collect():
    paths = collect_paths(config.paths)
    for path in paths:
        typer.echo(path)


@app.command()
def check():
    required_paths = collect_paths(config)
    missing_paths = get_missing_paths(required_paths)
    if missing_paths:
        printable_missing_paths = "\n".join(
            [str(p) for p in missing_paths if p.is_dir()]
        )
        typer.echo(
            f"Paths are missing:\n{printable_missing_paths}\nRun `arson init build` first."
        )
    else:
        typer.echo("All required directories are present")


@app.command()
def build():
    paths = collect_paths(config.paths)
    printable_paths = "\n".join([str(p) for p in paths if p.is_dir()])
    confirmation = typer.prompt(
        f"The following paths will be created.\n{printable_paths}\nConfirm? [Y/n]", "y"
    )
    if confirmation.lower() != "y":
        return

    for path in paths:
        match (path.is_dir(), path.exists()):
            case (True, True):
                typer.echo(f"Skipped: {path} (already exists)")
            case (True, False):
                path.mkdir(parents=True, exist_ok=True)
                typer.echo(f"Created: {path}")
            case _:
                pass


if __name__ == "__main__":
    app()
