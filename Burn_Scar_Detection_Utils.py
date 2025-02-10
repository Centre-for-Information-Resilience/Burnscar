import geemap as gee
import ee
import geopandas as gpd
import rasterio as rio
import numpy as np
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
from shapely.geometry import Point

# Ignore this - my proj path is messed up
# import pyproj
# pyproj.datadir.set_data_dir("/Users/***REMOVED***/miniconda3/envs/firms_automation/share/proj")

def get_data(firms_detections_gdf, index):
    lat = firms_detections_gdf.loc[index, "latitude"]
    lon = firms_detections_gdf.loc[index, "longitude"]
    point_geom = ee.Geometry.Point([lon, lat])
    event_date = firms_detections_gdf.loc[index, "acq_date"]
    village_polygon = firms_detections_gdf.geometry[index]
    buffered_point = point_geom.buffer(1000)
    square_aoi = buffered_point.bounds()
    return lat, lon, event_date, village_polygon, square_aoi, point_geom

def create_ee_geometries(village_polygon):
    coord_list = list(village_polygon.exterior.coords)
    ee_village_polygon = ee.Geometry.Polygon(coord_list)
    ee_village_polygon_bounds = ee_village_polygon.bounds()
    return ee_village_polygon, ee_village_polygon_bounds

def get_date_bounds(datestring):
  date_object = datetime.strptime(datestring, '%Y-%m-%d')
  range_begin = date_object - timedelta(days=30)
  range_begin_string = range_begin.strftime('%Y-%m-%d')
  range_end = date_object + timedelta(days=30)
  range_end_string = range_end.strftime('%Y-%m-%d')
  return range_begin_string, range_end_string

def add_dates(image):
  date = ee.Date(image.get('system:time_start')).format("yyyy-MM-dd")
  image = image.set('date', date)
  return image

def get_before_and_after_images(image_col, target_date, image_dates_list, clipping_area):
  target_date = datetime.strptime(target_date, '%Y-%m-%d')
  closest_before = min((date for date in image_dates_list if datetime.strptime(date, '%Y-%m-%d') < target_date), key=lambda date: target_date - datetime.strptime(date, '%Y-%m-%d'))
  closest_after = min((date for date in image_dates_list if datetime.strptime(date, '%Y-%m-%d') > target_date), key=lambda date: datetime.strptime(date, '%Y-%m-%d') - target_date)
  before_image = image_col.filter(ee.Filter.inList('date',ee.List([closest_before]))).first().clip(clipping_area)
  after_image = image_col.filter(ee.Filter.inList('date',ee.List([closest_after]))).first().clip(clipping_area)
  return before_image, after_image

def calc_NBR(image):
  NBR = image.expression(
                        '(NIR-SWIR)/(NIR+SWIR)', {
                          'NIR' : image.select('B8'),
                          'SWIR' : image.select('B12')
                        })
  NBR = NBR.rename('NBR')
  image = image.addBands(NBR)
  return image

def check_dates(target_date, dates_list):
    # Convert target date to datetime object
    target_date = datetime.strptime(target_date, '%Y-%m-%d')

    # Initialize flags for before and after
    before_target = False
    after_target = False

    # Iterate through the list of dates
    for date_str in dates_list:
        # Convert date string to datetime object
        date = datetime.strptime(date_str, '%Y-%m-%d')

        # Check if the date is before the target date
        if date < target_date:
            before_target = True
        # Check if the date is after the target date
        elif date > target_date:
            after_target = True

    # Return True if both before and after flags are True, else False
    return before_target and after_target

def calc_mean(image, geometry):
    mean_dict = image.reduceRegion(reducer = ee.Reducer.mean(), geometry = geometry, scale = 10)
    return mean_dict

def calc_count(image, geometry):
    count_dict = image.reduceRegion(reducer = ee.Reducer.count(), geometry = geometry, scale = 10)
    return count_dict

def check_cloud_cover(image,square_aoi):
  scl = image.select("SCL")
  clouds_mask = scl.gte(7).And(scl.lte(9))
  clouds = scl.updateMask(clouds_mask)
  cloudy_pixel_count = clouds.reduceRegion(reducer = ee.Reducer.count(), geometry = square_aoi, scale = 10)
  image = image.set({'num_cloudy_pixels': cloudy_pixel_count.getNumber('SCL')})
  return image

def check_intersection(featcol_1, featcol_2):
  spatialFilter = ee.Filter.intersects(leftField = '.geo', rightField ='.geo', maxError = 10 )
  saveAllJoin = ee.Join.saveAll(matchesKey = 'label')
  intersectJoined = saveAllJoin.apply(featcol_1, featcol_2, spatialFilter)
  return intersectJoined

def get_nearest_town(towns_gdf, detection_lat, detection_lon):
    detection_point = Point(detection_lon, detection_lat)
    point_gdf = gpd.GeoDataFrame(index=[0], crs="EPSG:4326", geometry=[detection_point])
    utm_crs = point_gdf.estimate_utm_crs()
    point_gdf_utm = point_gdf.to_crs(utm_crs)
    detection_point = point_gdf_utm.geometry[0]
    point_gdf_utm['geometry'] = point_gdf_utm.buffer(10000)
    towns_gdf_utm = towns_gdf.to_crs(utm_crs)
    nearby_towns = point_gdf_utm.overlay(towns_gdf_utm, keep_geom_type=False)
    if len(nearby_towns) > 1:
        for i in range(len(nearby_towns)):
            town_point_geom = nearby_towns.geometry[i]
            distance_to_detection_point = town_point_geom.distance(detection_point)
            nearby_towns.loc[i, 'distance_to_firms_detection'] = distance_to_detection_point
        nearby_towns = nearby_towns[nearby_towns['distance_to_firms_detection'] != 0]
        min_distance = np.min(nearby_towns['distance_to_firms_detection'])
        nearest_town_name = nearby_towns.loc[nearby_towns['distance_to_firms_detection'] == min_distance, "featureNam"]
        nearest_town_name = nearest_town_name.values[0]
    else:
        nearest_town_name = "Unknown Town"
    return nearest_town_name

def spatially_associate_arson_events(gdf):
    for i in range(len(gdf)):
        geom = gdf.geometry[i]
        wkt = geom.wkt
        gdf.loc[i, "wkt"] = wkt
        gdf.loc[i, "index"] = i
    unique_wkts = list(np.unique(gdf['wkt']))
    for i in range(len(unique_wkts)):
        unique_wkt = unique_wkts[i]
        subset_unique_wkt_gdf = gdf[gdf['wkt']==unique_wkt]
        unique_wkt_indices = list(subset_unique_wkt_gdf['index'])
        for unique_wkt_index in unique_wkt_indices:
            gdf.loc[unique_wkt_index, 'unique_event_no'] = i
    gdf = gdf.drop(columns = ['wkt', 'index'])
    return gdf    

def validate_FIRMS(df, index):
  output_data_dict = {}
  buildings = ee.FeatureCollection('GOOGLE/Research/open-buildings/v3/polygons').filter('confidence >= 0.75')
  s2_col = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED').filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE',10))
  lat, lon, event_date, village_polygon, square_aoi, point_geom = get_data(df, index)
  ee_village_polygon, ee_village_polygon_bounds = create_ee_geometries(village_polygon)
  village_buildings = buildings.filterBounds(square_aoi)
  start_date, end_date = get_date_bounds(event_date)
  filtered_s2_col = s2_col.filterDate(start_date, end_date).filterBounds(ee_village_polygon_bounds)
  filtered_s2_col = filtered_s2_col.map(add_dates)
  image_dates_list = filtered_s2_col.aggregate_array("date").getInfo()
  filtered_s2_col = filtered_s2_col.map(lambda image: check_cloud_cover(image, square_aoi))
  filtered_s2_col = filtered_s2_col.filter(ee.Filter.eq('num_cloudy_pixels', 0))
  cloud_free_image_dates_list = filtered_s2_col.aggregate_array("date").getInfo()
  if check_dates(event_date, cloud_free_image_dates_list) == True:
    output_data_dict['no_data'] = False
    before_image, after_image = get_before_and_after_images(filtered_s2_col, event_date, cloud_free_image_dates_list, square_aoi)
    before_image_with_NBR = calc_NBR(before_image)
    after_image_with_NBR = calc_NBR(after_image)
    before_NBR = before_image_with_NBR.select("NBR")
    after_NBR = after_image_with_NBR.select("NBR")
    NBR_difference = before_NBR.subtract(after_NBR)
    mask = NBR_difference.gte(0.15).And(after_NBR.lte(-0.10)) #these values can be changed to change the script sensitivity
    NBR_masked = NBR_difference.updateMask(mask)
    burnt_area_vector = mask.reduceToVectors(geometry = square_aoi, crs = NBR_masked.projection(), scale = 10, geometryType = 'polygon', eightConnected = False)
    burnt_area_vector = burnt_area_vector.filter(ee.Filter.eq('label', 1))
    burnt_buildings = check_intersection(village_buildings, burnt_area_vector)
    burnt_pixel_count = calc_count(NBR_masked, square_aoi).getInfo()['NBR']
    if burnt_pixel_count > 10: #This value can also be changed
      output_data_dict['burn_scar_detected'] = True
      num_burnt_buildings = burnt_buildings.size().getInfo()
      output_data_dict['lat'] = lat
      output_data_dict['lon'] = lon
      output_data_dict['event_date'] = event_date
      output_data_dict['before_image'] = before_image
      output_data_dict['after_image'] = after_image
      output_data_dict['before_NBR'] = before_NBR
      output_data_dict['after_NBR'] = after_NBR
      output_data_dict['burnt_area'] = NBR_masked
      output_data_dict['burnt_pixel_count'] = burnt_pixel_count
      output_data_dict['burnt_buildings'] = burnt_buildings
      output_data_dict['num_burnt_buildings'] = num_burnt_buildings
    else:
      output_data_dict['burn_scar_detected'] = False
  else:
    output_data_dict['no_data'] = True
    if check_dates(event_date, cloud_free_image_dates_list) == False and check_dates(event_date, image_dates_list) == True:
       output_data_dict['no_data_reason'] = "too cloudy"
    else:
       output_data_dict['no_data_reason'] = "no image"
  return output_data_dict