from fastapi import APIRouter, Depends, Query

from db.mongodb import get_database
from routers.dependencies import CommonFilters
from services import analytics_service

router = APIRouter()


@router.get("/general-info")
async def general_info(filters: CommonFilters = Depends(), db=Depends(get_database)):
    return await analytics_service.get_general_info(db, filters)


@router.get("/disease-info")
async def disease_info(
    diag_code: str = Query(..., pattern=r"^[A-Z]\d{2}$", description="3-character ICD-10 category, e.g. L23"),
    filters: CommonFilters = Depends(),
    db=Depends(get_database),
):
    return await analytics_service.get_disease_info(db, diag_code, filters)


@router.get("/temporal-info")
async def temporal_info(filters: CommonFilters = Depends(), db=Depends(get_database)):
    return await analytics_service.get_temporal_info(db, filters)
