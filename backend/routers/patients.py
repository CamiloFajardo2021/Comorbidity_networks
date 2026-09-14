from io import BytesIO

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import ValidationError

from db.mongodb import get_database
from routers.dependencies import CommonFilters, GraphFilters
from services import patient_service

router = APIRouter()


@router.get("/patients_path")
async def get_patients_path(
    filters: CommonFilters = Depends(),
    remove_diags: str | None = Query(default=None, max_length=200),
    db=Depends(get_database),
):
    # edge_weight_model / type_network aren't used by patients_path itself
    # (they only affect graph construction downstream), so GraphFilters is
    # built here with just the fields this endpoint actually needs - its
    # own defaults (CIJ / PATIENT) cover the rest. This also runs
    # remove_diags through GraphFilters' existing valid-regex validator.
    try:
        graph_filters = GraphFilters(filters=filters, remove_diags=remove_diags)
    except ValidationError as e:
        # e.g. remove_diags isn't a compilable regex
        raise HTTPException(status_code=422, detail=e.errors())

    df = await patient_service.patients_path(db, graph_filters)

    buffer = BytesIO()
    df.write_csv(buffer)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=patients_path.csv"},
    )
