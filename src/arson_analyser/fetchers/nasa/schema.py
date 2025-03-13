from datetime import date
from enum import StrEnum

from pydantic import BaseModel


class Satellite(StrEnum):
    SNPP = "N"
    NOAA20 = "N20"
    NOAA21 = "N21"


class Instrument(StrEnum):
    VIIRS = "VIIRS"


class Confidence(StrEnum):
    nominal = "n"
    low = "l"
    high = "h"


class DayNight(StrEnum):
    day = "D"
    night = "N"


class NASARecord(BaseModel):
    class Config:
        frozen = True

    country_id: str
    latitude: float
    longitude: float
    scan: float
    track: float
    acq_date: date
    acq_time: int
    satellite: Satellite
    instrument: Instrument
    version: str
    frp: float
    daynight: DayNight
    bright_ti4: float
    bright_ti5: float
    confidence: Confidence

    def __hash__(self):
        return hash(
            (
                self.country_id,
                self.latitude,
                self.longitude,
                self.acq_date,
                self.acq_time,
            )
        )
