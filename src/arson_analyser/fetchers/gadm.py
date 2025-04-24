import json
import logging
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)


def build_gadm_filename(country_id: str, level: int) -> str:
    return f"gadm41_{country_id}_{level}.json"


def fetch_gadm(country_id: str, level: int) -> dict:
    filename = build_gadm_filename(country_id=country_id, level=level)
    response = httpx.get("https://geodata.ucdavis.edu/gadm/gadm4.1/json/" + filename)
    response.raise_for_status()
    return response.json()


def ensure_gadm(path: Path, country_id: str, level: int) -> Path:
    full_path = path / build_gadm_filename(country_id, level)

    if not full_path.exists():
        try:
            topo = fetch_gadm(country_id, level)
            full_path.write_text(json.dumps(topo))

        except httpx.HTTPStatusError:
            raise ValueError(f"GADM level {level} not available for {country_id}.")

    return full_path


if __name__ == "__main__":
    ensure_gadm(Path("data/gadm"), "NLD", 2)
