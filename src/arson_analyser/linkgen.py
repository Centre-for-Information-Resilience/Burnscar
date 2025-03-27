import datetime
from urllib.parse import quote, urlencode


def copernicus(
    latitude: float,
    longitude: float,
    start_date: datetime.date,
    end_date: datetime.date,
) -> str:
    # https://browser.dataspace.copernicus.eu/?zoom=14&lat=13.10728&lng=25.31439&themeId=DEFAULT-THEME&fromTime=2025-01-23T00%3A00%3A00.000Z&toTime=2025-02-02T23%3A59%3A59.999Z&datasetId=S2_L2A_CDAS&layerId=6-SWIR&demSource3D=%22MAPZEN%22&cloudCoverage=30&dateMode=TIME%20RANGE
    base_url = "https://browser.dataspace.copernicus.eu/?"
    query = dict(
        zoom=14,
        lat=latitude,
        lng=longitude,
        themeId="DEFAULT-THEME",
        fromTime=f"{start_date}T00:00:00.000Z",
        toTime=f"{end_date}T23:59:59.999Z",
        datasetId="S2_L2A_CDAS",
        layerId="6-SWIR",
        demSource3D='"MAPZEN"',
        cloudCoverage=30,
        dateMode="TIME%20RANGE",
    )
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


if __name__ == "__main__":
    latitude = 52.070986
    longitude = 5.132678
    keyword = "Utrecht"
    start_date = datetime.date(2025, 1, 1)
    end_date = datetime.date(2025, 1, 15)

    # tests
    print(
        copernicus(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
        )
    )
    print(x(keyword=keyword, start_date=start_date, end_date=end_date))
    print(whopostedwhat(keyword=keyword, start_date=start_date, end_date=end_date))
