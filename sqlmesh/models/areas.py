import os

from sqlglot import exp

from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model


@model(
    "arson.areas_@{in_ex}",
    is_sql=True,
    kind="FULL",
    blueprints=[
        {"in_ex": "include", "path": "../geo/include.gpkg"},
        {"in_ex": "exclude", "path": "../geo/exclude.gpkg"},
    ],
    columns={"id": "char(32)", "geom": "geometry"},
)
def entrypoint(evaluator: MacroEvaluator) -> str | exp.Expression:
    path = evaluator.blueprint_var("path")
    if not path:
        raise ValueError("Path is required")

    if not os.path.exists(path):
        return "select null as id, null as geom limit 0"

    unnested = (
        exp.select("st_makevalid(unnest(st_dump(geom)).geom) as geom")
        .from_(f"st_read('{path}')")
        .subquery()
    )

    return exp.select("md5(st_aswkb(geom)) as id", "geom").from_(unnested)
