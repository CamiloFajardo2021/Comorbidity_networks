"""
Query filters shared across the analytics/graph/patients routers, per the
"Query Filter Parameters" table in docs/05_backend_api.md.

Used as `filters: CommonFilters = Depends()` in a route — FastAPI turns
each field on a Pydantic model used this way into its own query parameter,
so every router gets the same filters without redeclaring them.
"""

import re
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class CommonFilters(BaseModel):
    municipio: Optional[int] = None
    regimen: Optional[str] = Field(default=None, pattern=r"^\d$")
    sexo: Optional[str] = Field(default=None, pattern="^[FM]$")
    anio: Optional[str] = Field(default=None, pattern=r"^\d{4}$")
    edad_min: Optional[int] = Field(default=None, ge=0)
    edad_max: Optional[int] = Field(default=None, ge=0)
    tipo_evento: Optional[str] = Field(default=None, pattern="^[CUPH]$")

    # NOTE: docs/05_backend_api.md also lists `regimen` (C/S) as a filter,
    # but the current patient schema (schema_mongodb.txt) has no regimen
    # field on the document — the ETL doesn't capture it yet. Left out
    # here until that's resolved; adding it back is a one-line change.


class WeightEnum(str, Enum):
    CIJ = 'Co-ocurrence'
    SCI = 'SCI model'


class TypeEnum(str, Enum):
    PATIENT = 'Per-patient'
    CONSULT = 'Per-consult'


class GraphFilters(BaseModel):
    filters: CommonFilters
    edge_weight_model: WeightEnum = WeightEnum.CIJ
    type_network: TypeEnum = TypeEnum.PATIENT
    remove_diags: Optional[str] = None

    @field_validator("remove_diags")
    @classmethod
    def remove_diags_must_be_valid_regex(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        try:
            re.compile(value)
        except re.error as exc:
            raise ValueError(f"remove_diags is not a valid regex: {exc}")
        return value
