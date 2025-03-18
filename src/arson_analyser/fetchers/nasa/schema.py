from datetime import date, time
from enum import StrEnum

from pydantic import BaseModel, field_validator


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
    acq_time: time
    satellite: Satellite
    instrument: Instrument
    version: str
    frp: float
    daynight: DayNight
    bright_ti4: float
    bright_ti5: float
    confidence: Confidence

    @field_validator("acq_time", mode="before")
    def parse_time(cls, v):
        if isinstance(v, int):
            v_str = f"{v:04d}"  # Pad integer to ensure four digits
        elif isinstance(v, str):
            v_str = v.zfill(4)  # Handle strings, pad left zeros
        else:
            raise ValueError("Invalid time format")

        hour = int(v_str[:2])
        minute = int(v_str[2:])

        return time(hour=hour, minute=minute)

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
