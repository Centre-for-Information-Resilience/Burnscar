import os

from sqlglot import exp

from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model


@model(
    "arson.areas_@{in_ex}",
    is_sql=True,
    kind="VIEW",
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

    if path is None or not os.path.exists(path):
        return "select null as id, null as geom limit 0"

    file_mtime = os.path.getmtime(path)

    unnested = (
        exp.select("st_makevalid(unnest(st_dump(geom)).geom) as geom")
        .from_(f"st_read('{path}')")
        .subquery()
    )

    return exp.select(
        "md5(st_aswkb(geom)) as id",
        "geom",
        exp.Literal.number(file_mtime).as_("file_mtime"),
    ).from_(unnested)
