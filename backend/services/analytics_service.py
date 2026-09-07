"""
Aggregation logic behind the /analytics/* routes. Routers stay thin (parse
+ validate the request, call one of these, return the result) — all the
MongoDB query shape lives here, per the routers/ + services/ split in
docs/05_backend_api.md.
"""

import re

from routers.dependencies import CommonFilters
from services.query_utils import build_patient_match


async def get_general_info(db, filters: CommonFilters) -> dict:
    match = build_patient_match(filters)

    # tipo_evento filters *inside* the consultas array, so it's applied
    # per-consulta below rather than as a top-level $match key.
    consulta_cond = (
        {"$eq": ["$$c.tipo_evento", filters.tipo_evento]} if filters.tipo_evento else True
    )

    pipeline = [
        {"$match": match},
        # $facet runs both sub-pipelines against the same filtered patient
        # set in a single round trip to Mongo, instead of two queries.
        {"$facet": {
            "summary": [
                {"$project": {
                    "consulta_count": {
                        "$size": {
                            "$filter": {
                                "input": "$consultas",
                                "as": "c",
                                "cond": consulta_cond,
                            }
                        }
                    }
                }},
                {"$group": {
                    "_id": None,
                    "patient_count": {"$sum": 1},
                    "avg_consultations": {"$avg": "$consulta_count"},
                }},
            ],
            "top_diagnoses": [
                {"$unwind": "$consultas"},
                *([{"$match": {"consultas.tipo_evento": filters.tipo_evento}}] if filters.tipo_evento else []),
                {"$group": {"_id": "$consultas.diag_prin", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10},
            ],
        }},
    ]

    [result] = await db.patients.aggregate(pipeline).to_list(length=1)
    summary = result["summary"][0] if result["summary"] else {}

    return {
        "patient_count": summary.get("patient_count", 0),
        "avg_consultations_per_patient": round(summary.get("avg_consultations") or 0, 2),
        "top_diagnoses": [
            {"diagnosis": row["_id"], "count": row["count"]} for row in result["top_diagnoses"]
        ],
    }


async def get_disease_info(db, diag_code: str, filters: CommonFilters) -> dict:
    """Same shape as get_general_info, scoped to one diagnosis category
    (3-character ICD-10 prefix, e.g. "L23" matches "L239", "L230", ...) —
    matching the node-granularity choice made for the comorbidity graph."""
    diag_regex = f"^{re.escape(diag_code)}"

    match = build_patient_match(filters)
    # Restrict to patients who have at least one matching consulta. Mongo
    # checks this against every element of the consultas array for a
    # plain (non-$elemMatch) match on an array-of-subdocuments field.
    match["consultas.diag_prin"] = {"$regex": diag_regex}

    consulta_cond = {"$regexMatch": {"input": "$$c.diag_prin", "regex": diag_regex}}
    if filters.tipo_evento:
        consulta_cond = {"$and": [consulta_cond, {"$eq": ["$$c.tipo_evento", filters.tipo_evento]}]}

    pipeline = [
        {"$match": match},
        {"$facet": {
            "summary": [
                {"$project": {
                    "consulta_count": {
                        "$size": {"$filter": {"input": "$consultas", "as": "c", "cond": consulta_cond}}
                    }
                }},
                {"$group": {
                    "_id": None,
                    "patient_count": {"$sum": 1},
                    "avg_consultations": {"$avg": "$consulta_count"},
                }},
            ],
            "top_diagnoses": [
                {"$unwind": "$consultas"},
                {"$match": {
                    "consultas.diag_prin": {"$regex": diag_regex},
                    **({"consultas.tipo_evento": filters.tipo_evento} if filters.tipo_evento else {}),
                }},
                {"$group": {"_id": "$consultas.diag_prin", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10},
            ],
        }},
    ]

    [result] = await db.patients.aggregate(pipeline).to_list(length=1)
    summary = result["summary"][0] if result["summary"] else {}

    return {
        "diag_code": diag_code,
        "patient_count": summary.get("patient_count", 0),
        "avg_consultations_per_patient": round(summary.get("avg_consultations") or 0, 2),
        "top_specific_diagnoses": [
            {"diagnosis": row["_id"], "count": row["count"]} for row in result["top_diagnoses"]
        ],
    }


async def get_temporal_info(db, filters: CommonFilters) -> dict:
    """Average gap between a patient's consecutive consultas, and average
    procedure duration. Uses $setWindowFields + $shift (MongoDB 5.0+, so
    fine on mongo:8) to look at the *previous* consulta's date within each
    patient's own sorted timeline — the standard way to do this kind of
    "compare to the previous row" calculation in the aggregation pipeline,
    without pulling everything into Python to compute it by hand."""
    match = build_patient_match(filters)

    pipeline = [{"$match": match}, {"$unwind": "$consultas"}]
    if filters.tipo_evento:
        pipeline.append({"$match": {"consultas.tipo_evento": filters.tipo_evento}})

    pipeline += [
        {"$setWindowFields": {
            "partitionBy": "$_id",
            "sortBy": {"consultas.fecha": 1},
            "output": {
                "prev_fecha": {"$shift": {"output": "$consultas.fecha", "by": -1}},
            },
        }},
        {"$match": {"prev_fecha": {"$ne": None}}},
        {"$project": {
            "gap_days": {
                "$divide": [
                    {"$subtract": ["$consultas.fecha", "$prev_fecha"]},
                    1000 * 60 * 60 * 24,  # ms -> days
                ]
            },
            "dias_estancia": "$consultas.dias_estancia",
        }},
        {"$group": {
            "_id": None,
            "avg_days_between_consultations": {"$avg": "$gap_days"},
            "avg_procedure_duration_days": {"$avg": "$dias_estancia"},
        }},
    ]

    result = await db.patients.aggregate(pipeline).to_list(length=1)
    row = result[0] if result else {}

    return {
        "avg_days_between_consultations": round(row.get("avg_days_between_consultations") or 0, 2),
        "avg_procedure_duration_days": round(row.get("avg_procedure_duration_days") or 0, 2),
    }
