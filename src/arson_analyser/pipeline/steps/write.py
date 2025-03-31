from ...config import Config
from ...linkgen import add_links
from ...storage.base import BaseStorage
from ..sql import QueryLoader


def write(config: Config, storage: BaseStorage, ql: QueryLoader) -> None:
    # write validated detections to csv
    query = ql.load("select_output")
    output = storage.execute(query).df()
    output = add_links(output)  # add ocpernicus and social links
    output.to_csv(config.paths.output / "validated_detections.csv")

    # write clustered events to csv
    query = ql.load("select_clustered_events")
    storage.execute(query).to_csv(str(config.paths.output / "clustered_events.csv"))
