import subprocess
import sys
from pathlib import Path

import typer

from sqlmesh.core.context import Context

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


@app.command()
def export(path: Path = typer.Option(None)) -> None:
    ensure_sqlmesh_root()
    context = Context(paths=["."])
    engine = context.engine_adapter

    if not path:
        path = context.config.variables.get("path_output")

    try:
        with engine.connection as conn:
            table = context.resolve_table("mart.firms_validated")
            conn.execute(f"""
                            COPY {table}
                            TO '{path}/output.csv' WITH (
                            HEADER,
                            DELIMITER ',')
                        """)

            table = context.resolve_table("mart.firms_validated_clustered")
            conn.execute(f"""
                            COPY {table}
                            TO '{path}/output_clustered.csv' WITH (
                            HEADER,
                            DELIMITER ',')
                        """)
            typer.secho(f"Successfully stored outputs at {path}", fg="green")

    except Exception as e:
        typer.secho(f"Failed writing outputs: {e}", err=True, fg="red")
        sys.exit(1)


if __name__ == "__main__":
    app()
