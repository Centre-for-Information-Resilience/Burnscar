import re
from pathlib import Path

from pydantic import BaseModel, Field


class Query(BaseModel):
    name: str
    query: str
    parameter_names: list[str] = Field(default_factory=list)
    parameters: dict[str, str] = Field(default_factory=dict)

    def __str__(self) -> str:
        return f"Query({self.name}, {self.parameter_names})"


class QueryLoader:
    def __init__(self, queries_path: Path):
        self.queries_path = queries_path

    def load_query(self, name: str) -> Query:
        path = self.queries_path / f"{name}.sql"
        sql_string = path.read_text()
        params = re.findall(r"\$([A-z_]+)", sql_string)

        return Query(name=name, query=sql_string, parameter_names=params)


if __name__ == "__main__":
    loader = QueryLoader(Path("src/arson_analyser/pipeline/queries"))
    q = loader.load_query("create_firms")
    print(q)
