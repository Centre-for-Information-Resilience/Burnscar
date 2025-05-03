import os

from sqlglot import exp

from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model


@model(
    "arson.areas_@{in_ex}",
    is_sql=True,
    kind="FULL",
    blueprints=[
        {"in_ex": "include"},
        {"in_ex": "exclude"},
    ],
    columns={"id": "char(32)", "geom": "geometry"},
)
def entrypoint(evaluator: MacroEvaluator) -> str | exp.Expression:
    areas = evaluator.var("paths_areas")
    assert isinstance(areas, dict), "areas must be set in config.yaml"

    in_ex = evaluator.blueprint_var("in_ex")
    assert isinstance(in_ex, str), "in_ex must be set in blueprint"
    path = areas[in_ex]

    if not os.path.exists(path):
        return "select null as id, null as geom limit 0"

    unnested = (
        exp.select("st_makevalid(unnest(st_dump(geom)).geom) as geom")
        .from_(f"st_read('{path}')")
        .subquery()
    )

    return exp.select("md5(st_aswkb(geom)) as id", "geom").from_(unnested)
