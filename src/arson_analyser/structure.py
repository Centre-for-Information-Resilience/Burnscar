from pathlib import Path

from pydantic import BaseModel


def collect_paths(model: BaseModel, prefix: str = "") -> list[Path]:
    paths = []
    for name, field in model.model_fields.items():
        value = getattr(model, name)
        if isinstance(value, Path):
            paths.append(value)
        elif isinstance(value, BaseModel):
            paths.extend(collect_paths(value, prefix=f"{prefix}{name}."))
    return paths


def get_missing_paths(paths: list[Path]) -> list[Path]:
    missing_paths = []
    for path in paths:
        if path.is_dir() and not path.exists():
            missing_paths.append(path)

    return missing_paths


class StructureError(Exception):
    pass


def check(model: BaseModel):
    paths = collect_paths(model=model)
    missing_paths = get_missing_paths(paths)
    if missing_paths:
        raise StructureError(
            f"Paths are missing:{', '.join([str(p) for p in missing_paths])}"
        )
