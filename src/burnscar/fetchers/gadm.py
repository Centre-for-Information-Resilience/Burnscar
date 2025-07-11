import logging
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)


def get_gadm_filename(country_id: str) -> str:
    return f"gadm41_{country_id}.gpkg"


def fetch_gadm(country_id: str) -> bytes:
    base_url = "https://geodata.ucdavis.edu/gadm/gadm4.1/gpkg/"
    filename = get_gadm_filename(country_id)
    response = httpx.get(base_url + filename)
    response.raise_for_status()
    return response.content


def ensure_gadm(path: Path, country_id: str) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    full_path = path / get_gadm_filename(country_id)

    if not full_path.exists():
        try:
            topo = fetch_gadm(country_id)
            full_path.write_bytes(topo)

        except httpx.HTTPStatusError:
            raise ValueError(f"GADM not available for {country_id}.")

    return full_path


if __name__ == "__main__":
    ensure_gadm(Path("data/gadm"), "NLD")
