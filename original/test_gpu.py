import os

import cudf
import cuspatial
import ee
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from tqdm import tqdm

from arson_analyser.Burn_Scar_Detection_Utils import validate_FIRMS

# Use absolute paths
BASE_DIR = r"C:\***REMOVED***\work\CIR\Sudan Witness\FIRMS Automation WorkSpace\Arson_Analyser"
FIRMS_DATA_PATH = os.path.join(
    BASE_DIR, "Data", "FIRMS_Detections_2024_05_01_to_2024_05_19.geojson"
)
URBAN_FILTER_PATH = os.path.join(
    BASE_DIR, "Data", "Sudan_Filterer_Cleaned_Dissolved.gpkg"
)
OUTPATH_POLYGONS = os.path.join(
    BASE_DIR,
    "Outputs",
    "Validated_FIRMS_Detections_Town_Polygons_2024_04_01_to_2024_04_15.geojson",
)
OUTPATH_POINTS = os.path.join(
    BASE_DIR,
    "Outputs",
    "Validated_FIRMS_Detections_Points_2024_04_01_to_2024_04_15.geojson",
)

print("Loading FIRMS data...")
firms_data = gpd.read_file(FIRMS_DATA_PATH)
print("Loading urban filterer...")
filterer = gpd.read_file(URBAN_FILTER_PATH)

# Convert GeoPandas DataFrames to cuDF DataFrames
firms_cudf = cudf.DataFrame.from_pandas(firms_data)
filterer_cudf = cudf.DataFrame.from_pandas(filterer)

# Ensure geometries are in cuSpatial format
firms_points = cuspatial.GeoSeries.from_geopandas(firms_data.geometry)
filterer_polygons = cuspatial.GeoSeries.from_geopandas(filterer.geometry)

print("Performing Urban Filtering using GPU... this may take up to 20 minutes.")
urban_firms_detections = cuspatial.point_in_polygon(firms_points, filterer_polygons)
print(
    f"A total of {urban_firms_detections.sum()} fires detected by FIRMS in urban areas."
)

# Convert the results back to a GeoPandas DataFrame
urban_firms_detections_gdf = firms_data[urban_firms_detections.to_pandas()]

print("Exploding Urban Polygons...")
filterer_polygons_exploded = filterer.explode()

print("Associating urban areas with FIRMS detections...")
for i in tqdm(range(len(urban_firms_detections_gdf))):
    point_geom = urban_firms_detections_gdf.geometry.iloc[i]
    for index, row in filterer_polygons_exploded.iterrows():
        if row["geometry"].intersects(point_geom):
            intersecting_polygon = row["geometry"]
            urban_firms_detections_gdf.loc[i, "geometry"] = intersecting_polygon
            break

ee.Authenticate()
ee.Initialize(project="***REMOVED***")

print("Analysing each urban FIRMS detection for evidence of arson...")


def arson_analyser(urban_firms_detections_gdf):
    firms_detections = urban_firms_detections_gdf
    # Ensure columns exist and are of correct type
    if "too_cloudy" not in firms_detections.columns:
        firms_detections["too_cloudy"] = pd.Series(dtype="bool")
    if "burn_scar_detected" not in firms_detections.columns:
        firms_detections["burn_scar_detected"] = pd.Series(dtype="bool")

    for i in tqdm(range(len(firms_detections))):
        data_dict = validate_FIRMS(firms_detections, i)
        firms_detections.loc[i, "too_cloudy"] = data_dict["too_cloudy"]
        if not data_dict["too_cloudy"]:
            firms_detections.loc[i, "burn_scar_detected"] = data_dict[
                "burn_scar_detected"
            ]
            if data_dict["burn_scar_detected"]:
                firms_detections.loc[i, "burnt_pixel_count"] = data_dict[
                    "burnt_pixel_count"
                ]
                firms_detections.loc[i, "num_burnt_buildings"] = data_dict[
                    "num_burnt_buildings"
                ]
    firms_detections_points = firms_detections.copy()
    for i in range(len(firms_detections_points)):
        lat = firms_detections_points.loc[i, "latitude"]
        lon = firms_detections_points.loc[i, "longitude"]
        point_geom = Point(lon, lat)
        firms_detections_points.loc[i, "geometry"] = point_geom
    return firms_detections, firms_detections_points


validated_firms_polygons, validated_firms_detections_points = arson_analyser(
    urban_firms_detections_gdf
)


print("Performing spatial join...")
result_gdf = gpd.sjoin(selected_gdf, join_gdf, how="left", predicate="intersects")


validated_firms_polygons.to_file(OUTPATH_POLYGONS)
validated_firms_detections_points.to_file(OUTPATH_POINTS)
