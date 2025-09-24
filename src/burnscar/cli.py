import subprocess
import sys
from pathlib import Path

import typer

from sqlmesh.core.context import Context

from . import linkgen

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
def export(
    path: Path = typer.Option(None), add_links: bool = typer.Option(False)
) -> None:
    ensure_sqlmesh_root()
    context = Context(paths=["."])
    engine = context.engine_adapter

    if not path:
        path = context.config.variables.get("path_output")

    try:
        with engine.connection as conn:
            table = context.resolve_table("mart.firms_validated")
            output = conn.execute(f"SELECT * FROM {table}").fetchdf()

            table = context.resolve_table("mart.firms_validated_clustered")
            output_clustered = conn.execute(f"SELECT * FROM {table}").fetchdf()

            # add social links
            gadm_levels = [c for c in output.columns if c.startswith("gadm_")]
            keyword_cols = ["settlement_name"] + gadm_levels

            if add_links:
                output = linkgen.add_links(
                    output, id_columns=["firms_id"], keyword_cols=keyword_cols
                )
                output_clustered = linkgen.add_links(
                    output_clustered,
                    id_columns=["area_include_id", "event_no"],
                    keyword_cols=keyword_cols,
                )

            output.to_csv(f"{path}/output.csv")
            output_clustered.to_csv(f"{path}/output_clustered.csv")

            typer.secho(f"Successfully stored outputs at {path}", fg="green")

    except Exception as e:
        typer.secho(f"Failed writing outputs: {e}", err=True, fg="red")
        raise e


if __name__ == "__main__":
    app()
