import typing as t

from arson_analyser.linkgen import add_links
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import ModelKindName


@model(
    name="arson.output_to_disk",
    kind=ModelKindName.FULL,
    description="Final output view with added social links",
    columns={"firms_id": "int"},
)
def fetch_nasa_data(
    context: ExecutionContext,
    **kwargs: dict[str, t.Any],
) -> t.Generator[None, None, None]:
    output_table = context.resolve_table("arson.output")
    output_df = context.fetchdf(f"select * from {output_table}")

    # add social links
    gadm_levels = [c for c in output_df.columns if c.startswith("gadm_")]
    keyword_cols = ["settlement_name"] + gadm_levels
    output_with_links_df = add_links(output_df, keyword_cols=keyword_cols)

    # write to disk
    output_dir = context.var("path_output")
    assert isinstance(output_dir, str), "path_output needs to be defined in config.yaml"

    output_with_links_df.to_csv(output_dir + "/firms_output.csv", index=False)

    clustered_table = context.resolve_table("arson.firms_clustered")
    clustered_df = context.fetchdf(f"select * from {clustered_table}")
    clustered_df.to_csv(output_dir + "/output_clustered.csv", index=False)

    yield from ()
