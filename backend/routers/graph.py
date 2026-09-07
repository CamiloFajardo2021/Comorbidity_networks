from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import ValidationError

from db.mongodb import get_database
from routers.dependencies import CommonFilters, GraphFilters, TypeEnum, WeightEnum
from services import graph_service

router = APIRouter()


@router.get("")
async def get_graph(
    filters: CommonFilters = Depends(),
    edge_weight_model: WeightEnum = WeightEnum.CIJ,
    type_network: TypeEnum = TypeEnum.PATIENT,
    remove_diags: str | None = Query(default=None, max_length=200),
    format: str | None = Query(
        default=None,
        description='Omit for JSON (frontend rendering). Set to "gml" to download a file instead.',
    ),
    db=Depends(get_database),
):
    try:
        graphfilters = GraphFilters(
            filters=filters,
            edge_weight_model=edge_weight_model,
            type_network=type_network,
            remove_diags=remove_diags,
        )
    except ValidationError as e:
        # e.g. remove_diags isn't a compilable regex
        raise HTTPException(status_code=422, detail=e.errors())

    try:
        result = await graph_service.get_graph(db, graphfilters, download_format=format)
    except ValueError as e:
        # small-cell guard, or an unsupported download_format
        raise HTTPException(status_code=422, detail=str(e))

    if format is not None:
        return FileResponse(
            result["file_path"],
            media_type="application/octet-stream",
            filename=f"comorbidity_graph.{format}",
        )

    return result
