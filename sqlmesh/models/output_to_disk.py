import typing as t
from pathlib import Path

from arson.linkgen import add_links
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import ModelKindName


@model(
    name="arson.output_to_disk",
    kind=ModelKindName.FULL,
    description="Final outputs: output.csv (with added social links), output_clustered.csv (clusters of detections)",
    columns={"firms_id": "int"},
)
def write_outputs_to_disk(
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
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    output_with_links_df.to_csv(output_path / "firms_output.csv", index=False)

    clustered_table = context.resolve_table("arson.firms_clustered")
    clustered_df = context.fetchdf(f"select * from {clustered_table}")
    clustered_df.to_csv(output_path / "output_clustered.csv", index=False)

    yield from ()
