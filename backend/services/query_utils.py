"""
Mongo query-building logic shared across services/ — kept separate from
any one service so nothing has to import another service's private
helpers.
"""

from routers.dependencies import CommonFilters


def build_patient_match(filters: CommonFilters) -> dict:
    """Patient-level (top-of-document) $match stage — everything here is
    a field directly on the patient document, not inside `consultas`."""
    match: dict = {}

    if filters.municipio is not None:
        match["municipio"] = filters.municipio
    if filters.sexo is not None:
        match["sexo"] = filters.sexo
    if filters.anio is not None:
        match["anio"] = filters.anio
    if filters.edad_min is not None or filters.edad_max is not None:
        edad_range = {}
        if filters.edad_min is not None:
            edad_range["$gte"] = filters.edad_min
        if filters.edad_max is not None:
            edad_range["$lte"] = filters.edad_max
        match["edad"] = edad_range

    return match
