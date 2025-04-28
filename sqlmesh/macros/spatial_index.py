from sqlglot import Expression

from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator


@macro()
def create_spatial_index(
    evaluator: MacroEvaluator,
    model_name: Expression,
    column: str,
):
    if evaluator.runtime_stage == "creating":
        return f"CREATE INDEX {model_name.name}_{column}_idx ON {model_name} USING RTREE({column});"
    return None
