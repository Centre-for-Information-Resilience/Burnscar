import datetime
import json
import logging
import typing as t
from pathlib import Path

from ee import Initialize
from ee._helpers import ServiceAccountCredentials
from ee.featurecollection import FeatureCollection
from ee.filter import Filter
from ee.geometry import Geometry
from ee.image import Image
from ee.imagecollection import ImageCollection
from ee.join import Join
from ee.reducer import Reducer
from pydantic import BaseModel

from ..models import FireDetection
from ..utils import expect_type

logger = logging.getLogger(__name__)


def read_key(key_path: Path) -> dict[str, str]:
    return json.loads(key_path.read_text())


class ValidationImages(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    before: Image
    after: Image
    burnt_area: Image


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
    burnt_buildings: FeatureCollection | None = None

    # imagery
    images: ValidationImages | None = None

    # meta
    no_data: bool = False
    too_cloudy: bool = False


class GEEValidator:
    def __init__(self, key_path: Path):
        key = read_key(key_path)
        service_account = key["client_email"]
        project_id = key["project_id"]
        credentials = ServiceAccountCredentials(service_account, str(key_path))
        Initialize(credentials=credentials, project=project_id)

    @staticmethod
    def _get_buildings(filter_bounds: Geometry) -> FeatureCollection:
        buildings = (
            FeatureCollection("GOOGLE/Research/open-buildings/v3/polygons")
            .filter("confidence >= 0.75")
            .filterBounds(filter_bounds)
        )
        return buildings

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

        ee_aoi_bounds = self._get_ee_aoi_bounds(detection, buffer_distance)

        s2 = self._get_s2_collection(detection, days_around)

        image_dates = self._get_image_dates(image_collection=s2)

        # we stop early when there is no data from before and after the fire
        if not self._imagery_available(image_dates, detection.acq_date):
            result.no_data = True
            return result

        # filter out cloudy images
        s2 = s2.filter(Filter.lt("CLOUDY_PIXEL_PERCENTAGE", max_cloudy_percentage))
        image_dates = self._get_image_dates(image_collection=s2)

        # we also stop early when available imagery is too cloudy
        if not self._imagery_available(image_dates, detection.acq_date):
            result.too_cloudy = True
            return result

        # get nearest before and after image
        before, after = self._get_nearest_surrounding_dates(
            detection.acq_date, image_dates
        )

        # Get images for before and after dates
        before_image = self._get_image_for_date(ee_aoi_bounds, s2, before)
        after_image = self._get_image_for_date(ee_aoi_bounds, s2, after)

        # add Normalized Burn Ratio (NBR) to images
        before_image = self._add_NBR(before_image)
        after_image = self._add_NBR(after_image)

        # calculate difference in NBR and create a mask using some threshold
        nbr_difference = self._get_nbr_difference(before_image, after_image)
        nbr_mask = self._get_nbr_mask(
            after_image, nbr_difference, nbr_after_lte, nbr_difference_limit
        )

        nbr_masked = nbr_difference.updateMask(nbr_mask)

        # create vector of burnt areas
        burnt_area_vector = self._get_burnt_area_vector(
            ee_aoi_bounds, nbr_mask, nbr_masked
        )

        # filter buildings in Area of Interest
        buildings = self._get_buildings(ee_aoi_bounds)

        # spatially join buildings with burnt area vector
        burnt_buildings = self._get_burnt_buildings(burnt_area_vector, buildings)
        burnt_building_count = self._get_burnt_building_count(burnt_buildings)

        # count burnt pixels
        burnt_pixel_count = self._get_burnt_pixel_count(ee_aoi_bounds, nbr_masked)

        # build the result
        result.before_date = before
        result.after_date = after
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
    def _get_nbr_mask(
        after_image: Image,
        nbr_difference: Image,
        nbr_after_lte: float,
        nbr_difference_limit: float,
    ) -> Image:
        nbr_mask = nbr_difference.gte(nbr_difference_limit).And(
            after_image.select("NBR").lte(nbr_after_lte)
        )
        return nbr_mask

    @staticmethod
    def _get_nbr_difference(
        before_image: Image,
        after_image: Image,
    ) -> Image:
        nbr_difference: Image = before_image.select("NBR").subtract(
            after_image.select("NBR")
        )

        return nbr_difference

    @staticmethod
    def _get_burnt_area_vector(
        ee_aoi_bounds: Geometry,
        nbr_mask: Image,
        nbr_masked: Image,
    ) -> FeatureCollection:
        burnt_area_vector = nbr_mask.reduceToVectors(
            geometry=ee_aoi_bounds,
            crs=nbr_masked.projection(),
            scale=10,
            geometryType="polygon",
            eightConnected=False,
        ).filter(Filter.eq("label", 1))

        return burnt_area_vector

    @staticmethod
    def _get_burnt_buildings(
        burnt_area_vector: FeatureCollection,
        buildings: FeatureCollection,
    ) -> FeatureCollection:
        spatial_filter = Filter.intersects(
            leftField=".geo", rightField=".geo", maxError=10
        )
        burnt_buildings = Join.saveAll(matchesKey="label").apply(
            primary=buildings,
            secondary=burnt_area_vector,
            condition=spatial_filter,
        )

        return burnt_buildings

    @staticmethod
    def _get_burnt_building_count(
        burnt_buildings: FeatureCollection,
    ) -> int:
        burnt_building_count = burnt_buildings.size().getInfo()
        burnt_building_count = expect_type(burnt_building_count, int, 0)
        return burnt_building_count

    @staticmethod
    def _get_burnt_pixel_count(
        ee_aoi_bounds: Geometry,
        nbr_masked: Image,
    ) -> int:
        burnt_pixel_count = (
            nbr_masked.reduceRegion(
                reducer=Reducer.count(),
                geometry=ee_aoi_bounds,
                scale=10,
            )
            .get("NBR")
            .getInfo()
        )
        burnt_pixel_count = expect_type(burnt_pixel_count, int, 0)
        return burnt_pixel_count

    @staticmethod
    def _get_ee_aoi_bounds(
        detection: FireDetection,
        buffer_distance: int,
    ) -> Geometry:
        ee_point = Geometry.Point(detection.geom.x, detection.geom.y)

        # create square Area of Interest around fire detection
        ee_aoi_bounds = ee_point.buffer(distance=buffer_distance).bounds()
        return ee_aoi_bounds

    @staticmethod
    def _get_s2_collection(
        detection: FireDetection,
        days_around: int,
    ) -> ImageCollection:
        ee_area_include_bounds = Geometry.Polygon(
            list(detection.area_include_geom.exterior.coords)
        ).bounds()

        # fitler collections
        date_window = datetime.timedelta(days=days_around)
        start_date = str(detection.acq_date - date_window)
        end_date = str(detection.acq_date + date_window)

        s2: ImageCollection = (
            ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterDate(start_date, end_date)
            .filterBounds(ee_area_include_bounds)
        )

        return s2

    @staticmethod
    def _get_image_for_date(
        clipping_bounds: Geometry,
        image_collection: ImageCollection,
        date: datetime.date,
    ) -> Image:
        before_image = (
            image_collection.filterDate(
                str(date), str(date + datetime.timedelta(days=1))
            )
            .first()
            .clip(clipping_bounds)
        )

        return before_image

    @staticmethod
    def _get_image_dates(image_collection: ImageCollection) -> list[datetime.date]:
        image_dates = image_collection.aggregate_array("system:time_start").getInfo()
        image_dates = expect_type(image_dates, list, [])
        return list(
            map(lambda ts: datetime.date.fromtimestamp(ts / 1000.0), image_dates)
        )

    @staticmethod
    def _imagery_available(
        image_dates: list[datetime.date],
        target_date: datetime.date,
    ) -> bool:
        no_images = len(image_dates) == 0

        if no_images:
            return False

        before_and_after_image = min(image_dates) < target_date < max(image_dates)

        return before_and_after_image

    @staticmethod
    def _get_nearest_surrounding_dates(
        target_date: datetime.date,
        dates: list[datetime.date],
    ) -> tuple[datetime.date, datetime.date]:
        before = max((d for d in dates if d < target_date), default=min(dates))
        after = min((d for d in dates if d > target_date), default=max(dates))
        return before, after

    @staticmethod
    def _add_NBR(image: Image) -> Image:
        nbr = image.expression(
            "(NIR-SWIR)/(NIR+SWIR)",
            {"NIR": image.select("B8"), "SWIR": image.select("B12")},
        ).rename("NBR")
        image = image.addBands(nbr)
        return image
