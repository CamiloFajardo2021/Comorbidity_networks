from pydantic import BaseModel, Field, model_validator, field_validator
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from datetime import datetime, timezone
from enum import Enum
import os
from pathlib import Path
import polars as pl

LOG_DIR = Path(os.environ["LOG_PATH"]) 


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
    batch_int: int | None = None # outer: i_0 // N_ROWS for batch proccessing
    inner_batch_index: int | None = None  # which ops-chunk within that outer batch

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
        if not (2014 <= year <= 2024):
            raise ValueError(f"anio must be between 2014 and 2024, got {value!r}")
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

    def get_progress(self, municipio: str, anio: str):
        """Raw (status, batch_int, inner_batch_index) as last written for this
        municipio+anio. batch_int/inner_batch_index are None whenever that
        write never set them -- the final "fully done" marker never sets
        batch_int, which is exactly what is_fully_processed below relies on.
        Returns (None, None, None) when there's no log yet."""
        doc = self.collection.find_one(
            {"municipio": municipio, "anio": anio},
            {"status": 1, "batch_int": 1, "inner_batch_index": 1, "_id": 0},
        )
        if not doc:
            return None, None, None
        return LogStatus(doc["status"]), doc.get("batch_int"), doc.get("inner_batch_index")

    def is_fully_processed(self, municipio: str, anio: str) -> bool:
        """True only for the final "this municipio+anio is completely done"
        marker -- distinguished from a mid-run per-batch/per-chunk PROCESSED
        log by the absence of batch_int (every in-progress write sets it)."""
        status, batch_int, _ = self.get_progress(municipio, anio)
        return status == LogStatus.PROCESSED and batch_int is None

    def get_batch_int(self, municipio: str, anio: str) -> int:
        """Resume position for the inner BATCH=1000 mongo-insert loop used by
        get_df_final (the non-batched path). 0 when there's nothing to resume."""
        _, batch_int, _ = self.get_progress(municipio, anio)
        return batch_int if batch_int is not None else 0

    def write_log(self, log_format: LogsFormat):
        self.collection.update_one(
            {"municipio": log_format.municipio, "anio": log_format.anio},
            {"$set": log_format.model_dump()},
            upsert=True,
        )




class Binnacle:
    """
    csv format (pipe-delimited):
    |municipio|2014|2015|2016|2017|2018|2019|2020|2021|2022|2023|2024|status_final|
    """

    BINNACLE_FILE = LOG_DIR / "GENERAL_STATUS.csv"
    YEARS = [str(y) for y in range(2014, 2025)]  # 2014 ... 2024
    HEADER = ["municipio"] + YEARS + ["status_final"]

    def __init__(self):
        pass

    def check_(self) -> bool:
        return self.BINNACLE_FILE.exists()

    def build_(self) -> None:
        if not self.check_():
            self.BINNACLE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(self.BINNACLE_FILE, "w", encoding="utf-8") as file:
                file.write("|".join(self.HEADER) + "\n")

    def __built__(self) -> None:
        self.build_()

    def _read_rows(self) -> dict:
        """{municipio: {"2014": "0"/"1", ..., "status_final": "..."}}"""
        rows = {}
        with open(self.BINNACLE_FILE, "r", encoding="utf-8") as file:
            header = file.readline().strip().split("|")
            for line in file:
                line = line.strip()
                if not line:
                    continue
                record = dict(zip(header, line.split("|")))
                municipio = record.pop("municipio")
                rows[municipio] = record
        return rows

    def _write_rows(self, rows: dict) -> None:
        with open(self.BINNACLE_FILE, "w", encoding="utf-8") as file:
            file.write("|".join(self.HEADER) + "\n")
            for municipio, record in rows.items():
                line = (
                    [municipio]
                    + [record.get(y, "0") for y in self.YEARS]
                    + [record.get("status_final", "incomplete")]
                )
                file.write("|".join(line) + "\n")

    def check_municipio(self, municipio: str):
        self.__built__()  # make sure the file exists before reading it
        df = pl.read_csv(self.BINNACLE_FILE, separator="|", schema_overrides={"municipio": pl.Utf8})
        row = df.filter(pl.col("municipio") == municipio)

        if row.is_empty():
            return "no procesado"

        record = row.row(0, named=True)
        if record.get("status_final") == "complete":
            return "processed"

        # incomplete -> tell the caller which years are still missing
        return [y for y in self.YEARS if str(record.get(y)) != "1"]

    def insert_(self, municipio: str, anio: int) -> None:
        self.__built__()  # make sure the file exists before reading/writing
        anio_col = str(anio)
        if anio_col not in self.YEARS:
            raise ValueError(f"anio must be between {self.YEARS[0]} and {self.YEARS[-1]}, got {anio!r}")

        rows = self._read_rows()
        record = rows.get(municipio, {y: "0" for y in self.YEARS})

        record[anio_col] = "1"
        record["status_final"] = (
            "complete" if all(record.get(y) == "1" for y in self.YEARS) else "incomplete"
        )

        rows[municipio] = record
        self._write_rows(rows)








    