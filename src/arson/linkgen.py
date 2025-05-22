import datetime
from base64 import b64encode
from pathlib import Path
from urllib.parse import quote, urlencode

import pandas as pd

COPERNICUS_SCRIPT = (Path(__file__).parent / "copernicus.js").read_text()
COPERNICUS_SCRIPT_B64 = b64encode(COPERNICUS_SCRIPT.encode()).decode()


def gsheet_format(url: str, name: str) -> str:
    return f'=HYPERLINK("{url}","{name}")'


def copernicus(
    latitude: float,
    longitude: float,
    date: datetime.date,
    nbr: bool = False,
) -> str:
    ### Normal Copernicus link
    # https://browser.dataspace.copernicus.eu/
    # ?zoom=13
    # &lat=13.90832
    # &lng=31.22417
    # &themeId=DEFAULT-THEME
    # &datasetId=S2_L2A_CDAS
    # &fromTime=2025-04-21T00%3A00%3A00.000Z
    # &toTime=2025-04-21T23%3A59%3A59.999Z
    # &layerId=2_FALSE_COLOR
    # &demSource3D=%22MAPZEN%22
    # &cloudCoverage=20
    # &dateMode=SINGLE

    ### custom index link (NBR)
    # https://browser.dataspace.copernicus.eu/?zoom=14
    # &evalscript=Ly9WRVJTSU9OPTMKY29uc3QgY29sb3JSYW1wID0gW1swLDB4MDAwMDAwXSxbMC41LDB4OGY4ZjhmXSxbMSwweGZmZmZmZl1dCgpsZXQgdml6ID0gbmV3IENvbG9yUmFtcFZpc3VhbGl6ZXIoY29sb3JSYW1wKTsKCmZ1bmN0aW9uIHNldHVwKCkgewogIHJldHVybiB7CiAgICBpbnB1dDogWyJCMDgiLCJCMTIiLCAiZGF0YU1hc2siXSwKICAgIG91dHB1dDogWwogICAgICB7IGlkOiJkZWZhdWx0IiwgYmFuZHM6IDQgfSwKICAgICAgeyBpZDogImluZGV4IiwgYmFuZHM6IDEsIHNhbXBsZVR5cGU6ICdGTE9BVDMyJyB9CiAgICBdCiAgfTsKfQoKZnVuY3Rpb24gZXZhbHVhdGVQaXhlbChzYW1wbGVzKSB7CiAgbGV0IGluZGV4ID0gKHNhbXBsZXMuQjA4LXNhbXBsZXMuQjEyKS8oc2FtcGxlcy5CMDgrc2FtcGxlcy5CMTIpOwogIGNvbnN0IG1pbkluZGV4ID0gMDsKICBjb25zdCBtYXhJbmRleCA9IDE7CiAgbGV0IHZpc1ZhbCA9IG51bGw7CgogIGlmKGluZGV4ID4gbWF4SW5kZXggfHwgaW5kZXggPCBtaW5JbmRleCkgewogICAgdmlzVmFsID0gWzAsIDAsIDAsIDBdOwogIH0KICBlbHNlIHsKICAgIHZpc1ZhbCA9IFsuLi52aXoucHJvY2VzcyhpbmRleCksc2FtcGxlcy5kYXRhTWFza107CiAgfQoKICAvLyBUaGUgbGlicmFyeSBmb3IgdGlmZnMgb25seSB3b3JrcyB3ZWxsIGlmIHRoZXJlIGlzIG9uZSBjaGFubmVsIHJldHVybmVkLgogIC8vIFNvIGhlcmUgd2UgZW5jb2RlICJubyBkYXRhIiBhcyBOYU4gYW5kIGlnbm9yZSBOYU5zIG9uIHRoZSBmcm9udGVuZC4gIAogIGNvbnN0IGluZGV4VmFsID0gc2FtcGxlcy5kYXRhTWFzayA9PT0gMSA%2FIGluZGV4IDogTmFOOwoKICByZXR1cm4geyBkZWZhdWx0OiB2aXNWYWwsIGluZGV4OiBbaW5kZXhWYWxdIH07Cn0%3D
    # &handlePositions=0%2C0.56%2C1
    # &gradient=0x000000%2C0xffffff
    # &dateMode=SINGLE#custom-index

    base_url = "https://browser.dataspace.copernicus.eu/?"
    query = dict(
        zoom=14,
        lat=latitude,
        lng=longitude,
        themeId="DEFAULT-THEME",
        fromTime=f"{date}T00:00:00.000Z",
        toTime=f"{date}T23:59:59.999Z",
        datasetId="S2_L2A_CDAS",
        layerId="2_FALSE_COLOR",
        demSource3D='"MAPZEN"',
        cloudCoverage=20,
        dateMode="SINGLE",
    )
    if nbr:
        query["evalscript"] = COPERNICUS_SCRIPT_B64
        # query["handlePositions"] = "0,0.56,1"
        # query["gradient"] = "0x000000,0xffffff"
        query["dateMode"] = "SINGLE#custom-index"

    return base_url + urlencode(query)


def x(keyword: str, start_date: datetime.date, end_date: datetime.date) -> str:
    # https://x.com/search?lang=en&q=utrecht%20until%3A2025-01-02%20since%3A2025-01-01&src=typed_query
    base_url = "https://x.com/search?q="
    return base_url + quote(f'"{keyword}" since:{start_date} until:{end_date}')


def whopostedwhat(
    keyword: str,
    start_date: datetime.date,
    end_date: datetime.date,
):
    base_url = "https://www.facebook.com/search/posts/?"

    filters = dict(start_day=start_date, end_day=end_date)
    query = dict(q=keyword, filters=filters, epa="FILTERS")

    return base_url + urlencode(query)


def add_links(
    output: pd.DataFrame,
    id_columns: list[str] = ["firms_id"],
    date_buffer: int = 3,
    keyword_cols: list[str] = ["settlement_name", "gadm_1", "gadm_2", "gadm_3"],
) -> pd.DataFrame:
    rows = []
    for _, row in output.iterrows():
        start_date = row.get("start_date") or row["acq_date"]
        end_date = row.get("end_date") or row["acq_date"]
        start_date = (start_date - datetime.timedelta(days=date_buffer)).date()
        end_date = (end_date + datetime.timedelta(days=date_buffer)).date()

        links = row[id_columns].to_dict()

        links["link_copernicus_before"] = gsheet_format(
            copernicus(
                row["latitude"],
                row["longitude"],
                row["before_date"].date(),
            ),
            "s2_before",
        )

        links["link_copernicus_after"] = gsheet_format(
            copernicus(
                row["latitude"],
                row["longitude"],
                row["after_date"].date(),
            ),
            "s2_after",
        )

        links["link_copernicus_before_nbr"] = gsheet_format(
            copernicus(
                row["latitude"],
                row["longitude"],
                row["before_date"].date(),
                nbr=True,
            ),
            "s2_before_nbr",
        )

        links["link_copernicus_after_nbr"] = gsheet_format(
            copernicus(
                row["latitude"],
                row["longitude"],
                row["after_date"].date(),
                nbr=True,
            ),
            "s2_after_nbr",
        )

        for keyword_col in keyword_cols:
            links[f"link_{keyword_col}_x"] = gsheet_format(
                x(row[keyword_col], start_date, end_date), f"X_{keyword_col}"
            )

        rows.append(links)

    links_df = pd.DataFrame.from_records(rows)
    output = output.merge(links_df, on=id_columns)
    return output


if __name__ == "__main__":
    latitude = 12.19528
    longitude = 29.26756
    keyword = "Utrecht"
    start_date = datetime.date(2025, 4, 29)
    end_date = datetime.date(2025, 4, 29)

    # tests
    print(
        copernicus(
            latitude=latitude,
            longitude=longitude,
            date=start_date,
            nbr=True,
        )
    )
    print(x(keyword=keyword, start_date=start_date, end_date=end_date))
    print(whopostedwhat(keyword=keyword, start_date=start_date, end_date=end_date))
