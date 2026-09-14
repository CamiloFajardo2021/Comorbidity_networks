"""
Mongo query-building logic shared across services/ - kept separate from
any one service so nothing has to import another service's private
helpers.
"""

from routers.dependencies import CommonFilters


def build_patient_match(filters: CommonFilters) -> dict:
    """Patient-level (top-of-document) $match stage - everything here is
    a field directly on the patient document, not inside `consultas`."""
    match: dict = {}

    if filters.municipioAfiliacion is not None:
        match["municipioAfiliacion"] = filters.municipioAfiliacion

    if filters.municipioConsulta is not None:
        match["municipioConsulta"] = filters.municipioConsulta

    if filters.regimen is not None:
        # stored capitalized ("Regimen", from TipoUsuarioCD) - Mongo field
        # names are case-sensitive, so this must match exactly.
        match["Regimen"] = filters.regimen
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
    if filters.etnia is not None:
        match["etnia"] = filters.etnia
    if filters.zona is not None:
        match["zona"] = filters.zona
    if filters.actividad_economica is not None:
        match["actividad_economica"] = filters.actividad_economica
    if filters.discapacidad is not None:
        match["discapacidad"] = {"$in": filters.discapacidad}
    if filters.discapacidad_ant is not None:
        # stored under its raw BDUA column name - get_df_final never
        # aliases DescAntigRegistro to anything else.
        match["DescAntigRegistro"] = filters.discapacidad_ant

    return match
