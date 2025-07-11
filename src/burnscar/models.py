import datetime

from pydantic import BaseModel, field_validator
from shapely import Geometry, Point, Polygon, from_wkb


class FireDetection(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    firms_id: int
    acq_date: datetime.date
    geom: Point
    area_include_geom: Polygon

    @field_validator("geom", "area_include_geom", mode="before")
    def geom_validator(cls, v) -> Geometry:
        if isinstance(v, bytearray):
            v = bytes(v)

        return from_wkb(v)
