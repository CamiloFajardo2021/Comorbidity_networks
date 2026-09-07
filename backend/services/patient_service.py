"""
Builds the tidy (patient_id, consulta_id, diag_code) long-format table —
"patient_path" — that services/graph_service.py consumes, straight from
MongoDB. Nothing here aggregates: every (patient, consulta, diagnosis)
triple survives as its own row.

Filtering happens in two places, both pushed as close to the database as
possible: municipio/sexo/anio/edad via a $match (build_patient_match,
shared with analytics_service.py), and tipo_evento via a $filter on the
consultas array itself — so a patient whose only non-matching consultas
exist never has that data leave MongoDB at all. Only remove_diags (which
depends on the merged, 3-character-truncated diag_prin/diag_rel — a shape
that doesn't exist until after exploding) happens in Polars instead.
"""

import polars as pl

from routers.dependencies import GraphFilters
from services.query_utils import build_patient_match


async def patients_path(db, filters: GraphFilters) -> pl.DataFrame:
    common = filters.filters
    match = build_patient_match(common)

    consulta_cond = (
        {"$eq": ["$$c.tipo_evento", common.tipo_evento]} if common.tipo_evento else True
    )

    pipeline = [
        {"$match": match},
        # Only ever pull patient id (implicit) + consultas — every other
        # field (sexo, edad, etnia, ...) only existed to decide *which*
        # patients qualify, and that's already settled by $match above.
        {"$project": {
            "consultas": {
                "$filter": {"input": "$consultas", "as": "c", "cond": consulta_cond}
            }
        }},
        # drop patients left with zero consultas after that filter
        {"$match": {"consultas.0": {"$exists": True}}},
    ]

    docs = await db.patients.aggregate(pipeline).to_list(length=None)
    if not docs:
        return pl.DataFrame(schema={"patient_id": pl.Utf8, "consulta_id": pl.Int64, "diag_code": pl.Utf8})

    df = pl.DataFrame(docs)

    # --- one row per (patient, consulta) ---
    exploded = (
        df
        .rename({"_id": "patient_id"})
        .explode("consultas")
        .unnest("consultas")
    )

    # the raw data has no explicit consulta id — synthesize a stable
    # per-patient sequence number (0, 1, 2, ...) instead.
    exploded = exploded.with_columns(
        pl.int_range(pl.len()).over("patient_id").alias("consulta_id")
    )

    # --- one row per (patient, consulta, diagnosis): diag_prin and
    # diag_rel (already a list) merge into one list per consulta, then
    # explode that list. concat_list auto-wraps the scalar diag_prin
    # column as a single-element list before concatenating. ---
    long_df = (
        exploded
        .with_columns(
            pl.concat_list([pl.col("diag_prin"), pl.col("diag_rel")])
            .list.drop_nulls()
            .alias("diag_code")
        )
        .select("patient_id", "consulta_id", "diag_code")
        .explode("diag_code")
        # 3-character ICD-10 category, matching the graph's node
        # granularity (e.g. "L239" -> "L23").
        .with_columns(pl.col("diag_code").str.slice(0, 3))
    )

    # --- diagnosis exclusion regex from GraphFilters, e.g. "^(K1[0-6]|Z)" ---
    if filters.remove_diags is not None:
        long_df = long_df.filter(~pl.col("diag_code").str.contains(filters.remove_diags))

    return long_df
