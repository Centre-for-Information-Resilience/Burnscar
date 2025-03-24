import datetime

import ee
from pydantic import BaseModel, field_validator
from shapely import Geometry, Point, Polygon, from_wkb


class FireDetection(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    id: int
    acq_date: datetime.date
    geom: Point
    area_include_geom: Polygon

    @field_validator("geom", "area_include_geom", mode="before")
    def geom_validator(cls, v) -> Geometry:
        return from_wkb(v)


class ValidationResult(BaseModel):
    burn_scar_detected: bool = False
    burnt_pixel_count: int = 0
    burnt_building_count: int = 0

    no_data: bool = False
    too_cloudy: bool = False


class FIRMSValidator:
    def __init__(self, project: str):
        ee.Authenticate(auth_mode="gcloud")
        ee.Initialize(project=project)

        self._define_collections()

    def _define_collections(self) -> None:
        self.buildings: ee.featurecollection.FeatureCollection = ee.FeatureCollection(
            "GOOGLE/Research/open-buildings/v3/polygons"
        ).filter("confidence >= 0.75")
        self.s2: ee.imagecollection.ImageCollection = ee.ImageCollection(
            "COPERNICUS/S2_SR_HARMONIZED"
        ).filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 10))

    def validate(
        self,
        detection: FireDetection,
        buffer_distance: int = 1000,
        days_around: int = 30,
        max_cloudy_percentage: int = 20,
    ) -> ValidationResult:
        result = ValidationResult()

        ee_point = ee.Geometry.Point(detection.geom.x, detection.geom.y)

        # create square Area of Interest around fire detection
        ee_aoi_bounds = ee_point.buffer(distance=buffer_distance).bounds()

        # convert area polygon to ee Polygon
        ee_area_include_bounds = ee.Geometry.Polygon(
            list(detection.area_include_geom.exterior.coords)
        ).bounds()

        # fitler collections
        date_window = datetime.timedelta(days=days_around)
        start_date = str(detection.acq_date - date_window)
        end_date = str(detection.acq_date + date_window)

        s2: ee.imagecollection.ImageCollection = self.s2.filterDate(
            start_date, end_date
        ).filterBounds(ee_area_include_bounds)

        image_dates = self.get_image_dates(image_collection=s2)

        # we stop early when there is no data from before and after the fire
        if not min(image_dates) < detection.acq_date < max(image_dates):
            result.no_data = True
            return result

        # filter out cloudy images
        s2 = s2.filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", max_cloudy_percentage))
        image_dates = self.get_image_dates(image_collection=s2)

        # we also stop early when available imagery is too cloudy
        if not min(image_dates) < detection.acq_date < max(image_dates):
            result.too_cloudy = True
            return result

        # main validation logic starts here
        before, after = self.get_nearest_surrounding_dates(
            detection.acq_date, image_dates
        )
        before_image = (
            s2.filterDate(str(before), str(before + datetime.timedelta(days=1)))
            .first()
            .clip(ee_aoi_bounds)
        )

        after_image = (
            s2.filterDate(str(after), str(after + datetime.timedelta(days=1)))
            .first()
            .clip(ee_aoi_bounds)
        )

        before_image = self.add_NBR(before_image)
        after_image = self.add_NBR(after_image)

        nbr_difference = before_image.select("NBR").subtract(after_image.select("NBR"))
        after_nbr_limit = -0.10
        nbr_difference_limit = 0.15
        nbr_mask = nbr_difference.gte(nbr_difference_limit).And(
            after_image.select("NBR").lte(after_nbr_limit)
        )
        nbr_masked = nbr_difference.updateMask(nbr_mask)

        buildings = self.buildings.filterBounds(ee_aoi_bounds)

        return result

    @staticmethod
    def get_image_dates(
        image_collection: ee.imagecollection.ImageCollection,
    ) -> list[datetime.date]:
        image_dates = image_collection.aggregate_array("system:time_start").getInfo()
        return list(
            map(lambda ts: datetime.date.fromtimestamp(ts / 1000.0), image_dates)
        )

    @staticmethod
    def get_nearest_surrounding_dates(
        target_date: datetime.date, dates: list[datetime.date]
    ) -> tuple[datetime.date, datetime.date]:
        dates = sorted(dates)
        before, after = min(dates), max(dates)
        for date in dates:
            if date < target_date:
                before = date
            elif date > target_date:
                after = date
                break  # break the loop when we find the first date after the target

        return before, after

    @staticmethod
    def add_NBR(image: ee.image.Image):
        nbr = image.expression(
            "(NIR-SWIR)/(NIR+SWIR)",
            {"NIR": image.select("B8"), "SWIR": image.select("B12")},
        ).rename("NBR")
        image = image.addBands(nbr)
        return image
