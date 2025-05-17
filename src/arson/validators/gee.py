import datetime
import logging
import typing as t

import ee
from pydantic import BaseModel

from ..models import FireDetection
from ..utils import expect_type

logger = logging.getLogger(__name__)


class ValidationImages(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    before: ee.image.Image
    after: ee.image.Image
    burnt_area: ee.image.Image


class ValidationResult(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    firms_id: int
    acq_date: datetime.date
    before_date: datetime.date | None = None
    after_date: datetime.date | None = None
    burn_scar_detected: bool = False
    burnt_pixel_count: int = 0
    burnt_building_count: int = 0
    burnt_buildings: ee.featurecollection.FeatureCollection | None = None

    # imagery
    images: ValidationImages | None = None

    # meta
    no_data: bool = False
    too_cloudy: bool = False


class GEEValidator:
    def __init__(self, project: str, api_key: str):
        ee.Initialize(project=project, cloud_api_key=api_key)

        self._define_collections()

    def _define_collections(self) -> None:
        self.buildings: ee.featurecollection.FeatureCollection = ee.FeatureCollection(
            "GOOGLE/Research/open-buildings/v3/polygons"
        ).filter("confidence >= 0.75")
        self.s2: ee.imagecollection.ImageCollection = ee.ImageCollection(
            "COPERNICUS/S2_SR_HARMONIZED"
        ).filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 10))

    def validate_many(
        self,
        detections: list[FireDetection],
        validation_params: dict,
        max_workers: int = 10,
    ) -> t.Generator[ValidationResult, None, None]:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def safe_validate(detection: FireDetection) -> ValidationResult:
            try:
                return self.validate(detection, **validation_params)
            except Exception as e:
                logger.error(
                    f"Validation failed for FIRMS ID {detection.firms_id}: {e}"
                )
                return ValidationResult(
                    firms_id=detection.firms_id,
                    acq_date=detection.acq_date,
                    no_data=True,
                )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(safe_validate, det): det for det in detections}
            for future in as_completed(futures):
                yield future.result()

    def validate(
        self,
        detection: FireDetection,
        buffer_distance: int = 1000,
        days_around: int = 30,
        max_cloudy_percentage: int = 20,
        burnt_pixel_count_threshold: int = 10,
        nbr_after_lte: float = -0.10,
        nbr_difference_limit: float = 0.15,
    ) -> ValidationResult:
        result = ValidationResult(
            firms_id=detection.firms_id, acq_date=detection.acq_date
        )

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
        if not self.imagery_available(image_dates, detection.acq_date):
            result.no_data = True
            return result

        # filter out cloudy images
        s2 = s2.filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", max_cloudy_percentage))
        image_dates = self.get_image_dates(image_collection=s2)

        # we also stop early when available imagery is too cloudy
        if not self.imagery_available(image_dates, detection.acq_date):
            result.too_cloudy = True
            return result

        # get nearest before and after image
        before, after = self.get_nearest_surrounding_dates(
            detection.acq_date, image_dates
        )

        result.before_date = before
        result.after_date = after

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

        # add Normalized Burn Ratio (NBR) to images
        before_image = self.add_NBR(before_image)
        after_image = self.add_NBR(after_image)

        # calculate difference in NBR and create a mask using some threshold
        nbr_difference = before_image.select("NBR").subtract(after_image.select("NBR"))
        nbr_mask = nbr_difference.gte(nbr_difference_limit).And(
            after_image.select("NBR").lte(nbr_after_lte)
        )

        nbr_masked: ee.image.Image = nbr_difference.updateMask(nbr_mask)

        # create vector of burnt areas
        burnt_area_vector = nbr_mask.reduceToVectors(
            geometry=ee_aoi_bounds,
            crs=nbr_masked.projection(),
            scale=10,
            geometryType="polygon",
            eightConnected=False,
        ).filter(ee.filter.Filter.eq("label", 1))

        # filter buildings in Area of Interest
        buildings = self.buildings.filterBounds(ee_aoi_bounds)
        spatial_filter = ee.filter.Filter.intersects(
            leftField=".geo", rightField=".geo", maxError=10
        )

        # spatially join buildings with burnt area vector
        burnt_buildings = ee.join.Join.saveAll(matchesKey="label").apply(
            primary=buildings,
            secondary=burnt_area_vector,
            condition=spatial_filter,
        )
        burnt_building_count = burnt_buildings.size().getInfo()
        burnt_building_count = expect_type(burnt_building_count, int, 0)

        # count burnt pixels
        burnt_pixel_count = (
            nbr_masked.reduceRegion(
                reducer=ee.reducer.Reducer.count(),
                geometry=ee_aoi_bounds,
                scale=10,
            )
            .get("NBR")
            .getInfo()
        )
        burnt_pixel_count = expect_type(burnt_pixel_count, int, 0)

        # build the result
        result.burnt_pixel_count = burnt_pixel_count
        result.burnt_building_count = burnt_building_count
        result.burnt_buildings = burnt_buildings

        result.images = ValidationImages(
            before=before_image, after=after_image, burnt_area=nbr_masked
        )

        # if the number of burnt pixels is above our threshold we set this flag
        # NOTE: this is quite arbitrary, so it could also be moved to the manual
        # validation step / post processing so it remains adjustable.
        if burnt_pixel_count > burnt_pixel_count_threshold:
            result.burn_scar_detected = True

        return result

    @staticmethod
    def get_image_dates(
        image_collection: ee.imagecollection.ImageCollection,
    ) -> list[datetime.date]:
        image_dates = image_collection.aggregate_array("system:time_start").getInfo()
        image_dates = expect_type(image_dates, list, [])
        return list(
            map(lambda ts: datetime.date.fromtimestamp(ts / 1000.0), image_dates)
        )

    @staticmethod
    def imagery_available(
        image_dates: list[datetime.date], target_date: datetime.date
    ) -> bool:
        return (not len(image_dates) == 0) and (
            min(image_dates) < target_date < max(image_dates)
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
