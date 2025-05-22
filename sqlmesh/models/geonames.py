from pathlib import Path

from sqlglot import exp

from arson.fetchers.geonames import ensure_geonames
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import ModelKindName


@model(
    name="arson.geonames",
    kind=ModelKindName.FULL,
    is_sql=True,
    post_statements=[
        "@CREATE_SPATIAL_INDEX(@this_model, geom)",
    ],
)
def geonames(
    context: ExecutionContext,
) -> exp.Expression | str:
    country_id = context.var("country_id")
    assert country_id, "country_id not set in config"

    path_geonames = context.var("path_geonames")
    assert path_geonames and isinstance(path_geonames, str), (
        "path_geonames not set in config"
    )
    path_geonames = Path(path_geonames)

    # Ensure geonames exists
    full_path_geonames = ensure_geonames(
        path=path_geonames,
        country_id=country_id,
    )

    return f"""
        SELECT
            name::text,
            st_point(longitude, latitude)::geometry as geom,

        FROM read_csv('{full_path_geonames}', names = [
            'geonameid',
            'name',
            'asciiname',
            'alternatenames',
            'latitude',
            'longitude',
            'feature_class',
            'feature_code',
            'country_code',
            'cc2',
            'admin1_code',
            'admin2_code',
            'admin3_code',
            'admin4_code',
            'population',
            'elevation',
            'dem',
            'timezone',
            'modification_date'])
        """
