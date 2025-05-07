from pathlib import Path

from sqlglot import exp

from arson_analyser.fetchers.gadm import ensure_gadm
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import ModelKindName


@model(
    name="arson.gadm",
    kind=ModelKindName.FULL,
    is_sql=True,
    post_statements=[
        "@CREATE_SPATIAL_INDEX(@this_model, geom)",
    ],
)
def gadm(
    context: ExecutionContext,
) -> exp.Expression:
    country_id = context.var("country_id")
    assert country_id, "country_id not set in config"

    gadm_level = context.var("gadm_level")
    assert gadm_level, "gadm_level not set in config"
    gadm_level = int(gadm_level)
    assert gadm_level in (1, 2, 3), "gadm_level must be 1, 2 or 3"

    path_gadm = context.var("path_gadm")
    assert path_gadm and isinstance(path_gadm, str), "path_gadm not set in config"
    path_gadm = Path(path_gadm)

    # Ensure gadm exists
    full_path_gadm = ensure_gadm(
        path=path_gadm,
        country_id=country_id,
    )

    gadm_levels = [
        exp.cast(exp.column("NAME_1"), "text").as_("gadm_1"),
        exp.cast(exp.column("NAME_2"), "text").as_("gadm_2"),
        exp.cast(exp.column("NAME_3"), "text").as_("gadm_3"),
    ]

    return exp.select(
        exp.cast(exp.column("GID_0"), "text").as_("country_id"),
        *gadm_levels[:gadm_level],
        exp.cast(exp.column("geom"), "geometry").as_("geom"),
    ).from_(f"st_read('{full_path_gadm}', layer='ADM_ADM_{gadm_level}')")
