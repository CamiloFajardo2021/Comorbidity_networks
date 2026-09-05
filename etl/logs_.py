from pydantic import BaseModel, Field, model_validator, field_validator
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from datetime import datetime, timezone
from enum import Enum




class LogStatus(str, Enum):
    PROCESSED = "processed"
    NOT_PROCESSED = "not_processed"
    ERROR = "error"



class LogsFormat(BaseModel):
    municipio: str = Field(pattern=r"^\d{5}$")
    anio: str = Field(pattern=r"^\d{4}$")   # exactly 4 digits, e.g. "2014"
    status: LogStatus = LogStatus.NOT_PROCESSED
    date: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error: str | None = None
    batch_int: int | None = None

    @model_validator(mode="after")
    def _check_error_field(self):
        if self.status == LogStatus.ERROR and not self.error:
            raise ValueError("error must be set when status is ERROR")
        return self

    @model_validator(mode="after")
    def _check_batch_int_field(self):
        if self.status == LogStatus.ERROR and self.batch_int is None:
            raise ValueError("batch_int must be set when status is ERROR")
        return self

    @field_validator("anio")
    @classmethod
    def anio_in_range(cls, value: str) -> str:
        year = int(value)
        if not (2014 <= year <= 2023):
            raise ValueError(f"anio must be between 2014 and 2023, got {value!r}")
        return value


class LogsMongoDB:

    COLECTION_LOGS_NAME = 'Logs'

    def __init__(self,db):
        self.client = db
        self.collection = db[self.COLECTION_LOGS_NAME]

    def get_status(self, municipio: str, anio: str) -> LogStatus | None:
        doc = self.collection.find_one(
            {"municipio": municipio, "anio": anio},
            {"status": 1, "_id": 0},
        )
        return LogStatus(doc["status"]) if doc else None

    def get_batch_int(self, municipio: str, anio: str) -> int:
        doc = self.collection.find_one(
            {"municipio": municipio, "anio": anio},
            {"batch_int": 1, "_id": 0},
        )
        return doc.get("batch_int", 0) if doc else 0

    def write_log(self, log_format: LogsFormat):
        self.collection.update_one(
            {"municipio": log_format.municipio, "anio": log_format.anio},
            {"$set": log_format.model_dump()},
            upsert=True,
        )







    