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
    municipioAfiliacion: Optional[int] = None 
    municipioConsulta: Optional[int] = None
    regimen: Optional[str] = Field(default=None, pattern=r"^\d$")
    sexo: Optional[str] = Field(default=None, pattern="^[FM]$")
    anio: Optional[str] = Field(default=None, pattern=r"^\d{4}$")
    edad_min: Optional[int] = Field(default=None, ge=0)
    edad_max: Optional[int] = Field(default=None, ge=0)
    tipo_evento: Optional[str] = Field(default=None, pattern="^[CUPH]$")
    # TODO: "^[FM]$" was copy-pasted from `sexo` and is wrong for ethnicity
    # codes - swap in the real valid values once you have them; max_length
    # alone (matching actividad_economica's style) is a safe placeholder.
    etnia: Optional[str] = Field(default=None, max_length=30)
    zona: Optional[str] = Field(default=None, pattern="^[UR]$")
    actividad_economica: Optional[str] = Field(default=None, max_length=30)
    discapacidad: Optional[list[str]] = None
    discapacidad_ant: Optional[str] = None

    @field_validator("discapacidad", mode="before")
    @classmethod
    def split_discapacidad_csv(cls, value):
        # Lets ?discapacidad=fisica,visual work as shorthand alongside the
        # standard repeated-param form ?discapacidad=fisica&discapacidad=visual
        # (both are valid ways to send a list to a Depends()-flattened field).
        if isinstance(value, str):
            return [v.strip() for v in value.split(",") if v.strip()]
        return value


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
