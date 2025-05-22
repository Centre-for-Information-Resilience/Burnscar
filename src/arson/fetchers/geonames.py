import io
import logging
import zipfile
from pathlib import Path

import httpx
import pycountry

logger = logging.getLogger(__name__)


def iso3_to_iso2(country_id: str) -> str:
    """
    Convert ISO 3166-1 alpha-3 country code to ISO 3166-1 alpha-2 country code.
    """
    country = pycountry.countries.get(alpha_3=country_id)
    if country is None:
        raise LookupError(f"Country with ID {country_id} not found.")
    return country.alpha_2


def fetch_geonames(iso2: str) -> bytes:
    base_url = "https://download.geonames.org/export/dump/"
    filename = f"{iso2}.zip"
    response = httpx.get(base_url + filename)
    response.raise_for_status()
    return response.content


def ensure_geonames(path: Path, country_id: str) -> Path:
    iso2 = iso3_to_iso2(country_id)
    path.mkdir(parents=True, exist_ok=True)
    full_path = (path / country_id).with_suffix(".txt")

    if not full_path.exists():
        try:
            geonames = fetch_geonames(iso2)
            with zipfile.ZipFile(io.BytesIO(geonames)) as zf:
                contents = zf.open(name=f"{iso2}.txt").read()
                full_path.write_bytes(contents)
                logger.info(f"Downloaded geonames for {country_id} to {full_path}")

        except httpx.HTTPStatusError:
            raise ValueError(f"Geonames not available for {country_id}.")

    return full_path


if __name__ == "__main__":
    ensure_geonames(Path("data/geonames"), "NLD")
