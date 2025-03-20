import base64
import datetime
import json
import os
import urllib.parse

import ee
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from tqdm import tqdm

from .Burn_Scar_Detection_Utils import (
    get_nearest_town,
    spatially_associate_arson_events,
    validate_FIRMS,
)

# Use absolute paths
BASE_DIR = r"***REMOVED***"
FIRMS_DATA_PATH = os.path.join(BASE_DIR, "Data", "data.csv")
URBAN_FILTER_PATH = os.path.join(
    BASE_DIR, "Data", "Sudan_Filterer_Cleaned_Dissolved.gpkg"
)
SETTLEMENTS_PATH = os.path.join(BASE_DIR, "Data", "settlements.gpkg")
JOIN_PATH = os.path.join(BASE_DIR, "Data", "gadm41_SDN_3.shp")
OUTPATH_POLYGONS = os.path.join(
    BASE_DIR, "Outputs", "Validated_FIRMS_Detections_Town_Polygons.geojson"
)
OUTPATH_POINTS = os.path.join(
    BASE_DIR, "Outputs", "Validated_FIRMS_Detections_Points.geojson"
)
OUTPATH_CSV = os.path.join(BASE_DIR, "Outputs", "Validated_FIRMS_Detections_Points.csv")


print("Loading FIRMS data...")
firms_data_df = gpd.read_file(FIRMS_DATA_PATH)

# Ensure latitude and longitude are floats
firms_data_df["latitude"] = firms_data_df["latitude"].astype(float)
firms_data_df["longitude"] = firms_data_df["longitude"].astype(float)

# Create geometry column
firms_data_df["geometry"] = gpd.points_from_xy(
    firms_data_df.longitude, firms_data_df.latitude
)

# Convert to GeoDataFrame and set CRS
firms_data = gpd.GeoDataFrame(firms_data_df, geometry="geometry", crs="EPSG:4326")

# Validate geometries
print("Validating geometries...")
invalid_geometries = firms_data[~firms_data.is_valid]
if not invalid_geometries.empty:
    print(f"Found {len(invalid_geometries)} invalid geometries. Fixing them...")
    firms_data = firms_data.buffer(0)  # Fix invalid geometries by buffering

print("FIRMS data loaded and validated successfully.")
print("Loading settlememt data...")
settlement_data = gpd.read_file(SETTLEMENTS_PATH)
print("Loading urban filterer...")
filterer = gpd.read_file(URBAN_FILTER_PATH)
print("Loading Join Dataset")
join_gdf = gpd.read_file(JOIN_PATH)
print("Performing Urban Filtering... this may take up to 20 minutes.")
urban_firms_detections = firms_data.overlay(filterer, how="intersection")
print(
    f"A total of {len(urban_firms_detections)} fires detected by FIRMS in urban areas."
)
print("Exploding Urban Polygons...")
filterer_polygons = filterer.explode()

print("Associating urban areas with FIRMS detections...")
for i in tqdm(range(len(urban_firms_detections))):
    point_geom = urban_firms_detections.geometry[i]
    for index, row in filterer_polygons.iterrows():
        if row["geometry"].intersects(point_geom):
            intersecting_polygon = row["geometry"]
            urban_firms_detections.loc[i, "geometry"] = intersecting_polygon
            break

ee.Authenticate()
ee.Initialize(project="***REMOVED***")

print("Analysing each urban FIRMS detection for evidence of arson...")


def arson_analyser(urban_firms_detections_gdf):
    firms_detections = urban_firms_detections_gdf
    # Ensure columns exist and are of correct type
    if "no_data" not in firms_detections.columns:
        firms_detections["no_data"] = pd.Series(dtype="bool")
    if "burn_scar_detected" not in firms_detections.columns:
        firms_detections["burn_scar_detected"] = pd.Series(dtype="bool")

    for i in tqdm(range(len(firms_detections))):
        data_dict = validate_FIRMS(firms_detections, i)
        firms_detections.loc[i, "no_data"] = data_dict["no_data"]
        if data_dict["no_data"] == False:
            firms_detections.loc[i, "burn_scar_detected"] = data_dict[
                "burn_scar_detected"
            ]
            if data_dict["burn_scar_detected"] == True:
                firms_detections.loc[i, "burnt_pixel_count"] = data_dict[
                    "burnt_pixel_count"
                ]
                firms_detections.loc[i, "num_burnt_buildings"] = data_dict[
                    "num_burnt_buildings"
                ]
        else:
            firms_detections.loc[i, "no_data_reason"] = data_dict["no_data_reason"]
    firms_detections = spatially_associate_arson_events(
        firms_detections
    )  # new function to associate detections within the same town
    firms_detections_points = firms_detections.copy()
    for i in range(len(firms_detections_points)):
        lat = firms_detections_points.loc[i, "latitude"]
        lon = firms_detections_points.loc[i, "longitude"]
        point_geom = Point(lon, lat)
        urban_area_name = get_nearest_town(settlement_data, lat, lon)
        firms_detections_points.loc[i, "geometry"] = point_geom
        firms_detections_points.loc[i, "urban_area_name"] = urban_area_name
    return firms_detections, firms_detections_points


validated_firms_polygons, validated_firms_detections_points = arson_analyser(
    urban_firms_detections
)


print("Performing spatial join...")
validated_firms_detections_join = gpd.sjoin(
    validated_firms_detections_points, join_gdf, how="left", predicate="intersects"
)

# Specify the columns to include in the final CSV
columns_to_include = [
    "country_id",
    "NAME_1",
    "NAME_2",
    "NAME_3",
    "urban_area_name",
    "latitude",
    "longitude",
    "acq_date",
    "unique_event_no",
    "no_data",
    "no_data_reason",
    "burn_scar_detected",
]

# Drop the geometry column and select only the specified columns
validated_firms_detections_final = validated_firms_detections_join.drop(
    columns="geometry", axis=1
)[columns_to_include]


# Generate analysis links
def generate_links(validated_firms_detections_final):
    for index, row in validated_firms_detections_final.iterrows():
        center_date = datetime.datetime.strptime(row["acq_date"], "%Y-%m-%d")
        start_date = (center_date - datetime.timedelta(days=5)).strftime("%Y-%m-%d")
        end_date = (center_date + datetime.timedelta(days=5)).strftime("%Y-%m-%d")

        eo_browser_link = f"https://apps.sentinel-hub.com/eo-browser/?zoom=14&lat={row['latitude']}&lng={row['longitude']}&themeId=DEFAULT-THEME&visualizationUrl=https%3A%2F%2Fservices.sentinel-hub.com%2Fogc%2Fwms%2Fbd86bcc0-f318-402b-a145-015f85b9427e&datasetId=S2L2A&fromTime={start_date}T00%3A00%3A00.000Z&toTime={end_date}T23%3A59%3A59.999Z&layerId=6-SWIR&demSource3D=%22MAPZEN%22"

        def generate_social_links(keyword):
            encoded_keyword = urllib.parse.quote(keyword)

            twitter_link = f"https://x.com/search?q={encoded_keyword}%20since%3A{start_date}%20until%3A{end_date}"

            args_json = {
                "start_year": str(center_date.year),
                "start_month": f"{center_date.year}-{center_date.month}",
                "end_year": str(center_date.year),
                "end_month": f"{center_date.year}-{center_date.month}",
                "start_day": start_date,
                "end_day": end_date,
            }
            escaped_args = json.dumps(args_json).replace('"', '\\"')
            inner_json_str = f'{{"name": "creation_time", "args": "{escaped_args}"}}'
            outer_json = {"rp_creation_time": inner_json_str}
            base64_encoded = base64.b64encode(json.dumps(outer_json).encode()).decode()
            filters = urllib.parse.quote(base64_encoded)

            whopostedwhat_link = f"https://www.facebook.com/search/posts/?q={encoded_keyword}&filters={filters}&epa=FILTERS"

            return twitter_link, whopostedwhat_link

        if pd.notna(row["urban_area_name"]):
            pau_twitter_link, pau_whopostedwhat_link = generate_social_links(
                row["urban_area_name"]
            )
            validated_firms_detections_final.at[index, "vil_Twitter_Link"] = (
                pau_twitter_link
            )
            validated_firms_detections_final.at[index, "vil_Whopostedwhat_Link"] = (
                pau_whopostedwhat_link
            )
        else:
            validated_firms_detections_final.at[index, "vil_Twitter_Link"] = None
            validated_firms_detections_final.at[index, "vil_Whopostedwhat_Link"] = None

        if pd.notna(row["NAME_2"]):
            au_twitter_link, au_whopostedwhat_link = generate_social_links(
                row["NAME_2"]
            )
            validated_firms_detections_final.at[index, "ADM2_Twitter_Link"] = (
                au_twitter_link
            )
            validated_firms_detections_final.at[index, "ADM2_Whopostedwhat_Link"] = (
                au_whopostedwhat_link
            )
        else:
            validated_firms_detections_final.at[index, "ADM2_Twitter_Link"] = None
            validated_firms_detections_final.at[index, "ADM2_Whopostedwhat_Link"] = None

        if pd.notna(row["NAME_3"]):
            loc_twitter_link, loc_whopostedwhat_link = generate_social_links(
                row["NAME_3"]
            )
            validated_firms_detections_final.at[index, "ADM1_Twitter_Link"] = (
                loc_twitter_link
            )
            validated_firms_detections_final.at[index, "ADM1_Whopostedwhat_Link"] = (
                loc_whopostedwhat_link
            )
        else:
            validated_firms_detections_final.at[index, "ADM1_Twitter_Link"] = None
            validated_firms_detections_final.at[index, "ADM1_Whopostedwhat_Link"] = None

        validated_firms_detections_final.at[index, "EO_Browser_Link"] = eo_browser_link


generate_links(validated_firms_detections_final)


validated_firms_detections_final.to_csv(
    OUTPATH_CSV, index=False, encoding="utf-8-sig", lineterminator="\n"
)
validated_firms_polygons.to_file(OUTPATH_POLYGONS)
validated_firms_detections_join.to_file(OUTPATH_POINTS)
