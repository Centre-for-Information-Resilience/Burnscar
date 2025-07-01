import subprocess
import sys
from pathlib import Path

import typer

app = typer.Typer(name="burnscar", help="CLI for Burnscar, a SQLMesh project.")


def ensure_sqlmesh_root():
    config_path = Path("config.yaml")
    if not config_path.exists():
        typer.secho(
            "Error: Must be run from `sqlmesh/` (where config.yaml is)",
            fg=typer.colors.RED,
        )
        sys.exit(1)


@app.command()
def init(env: str = typer.Argument("prod", help="Environment to initialize")):
    """
    Initialize the SQLMesh environment and apply the initial plan.
    """
    ensure_sqlmesh_root()
    typer.echo(f"Initializing SQLMesh environment: {env}")
    subprocess.run(["sqlmesh", "plan", env, "--auto-apply"], check=True)


@app.command()
def run():
    """
    Run the SQLMesh DAG for the specified environment.
    """
    ensure_sqlmesh_root()
    subprocess.run(["sqlmesh", "run"], check=True)


if __name__ == "__main__":
    app()
