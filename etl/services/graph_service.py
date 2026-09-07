"""
Builds NetworkX comorbidity graphs from patients_path() output, and
services the /graph route: get_graph() decides between two very different
jobs behind one function — build (or reuse a cached) file for download,
or build in memory and hand back JSON for the frontend to render — while
sharing one cache key between them, since the underlying graph is
identical either way.
"""

import hashlib
import json
import logging
import math
import os
from collections import Counter
from itertools import combinations
from pathlib import Path

import networkx as nx
import polars as pl

from routers.dependencies import GraphFilters, TypeEnum, WeightEnum
from services.patient_service import patients_path

logger = logging.getLogger(__name__)

CACHE_DIR = Path(os.environ.get("CACHE_DIR", "/cache"))
# Statistical disclosure control: refuse to build/return a graph built
# from too few real patients, since a narrow enough filter combination
# can make an "aggregate" graph re-identifying.
MIN_PATIENT_COUNT = int(os.environ.get("MIN_PATIENT_COUNT", 5))


# --------------------------------------------------------------------------
# graph construction (CIJ / SCI)
# --------------------------------------------------------------------------

def _pairwise_cooccurrence_counts(patient_path: pl.DataFrame, group_col: str) -> Counter:
    """C_ij: how many groups (patients, or consultas — depends on
    group_col) contain both diagnosis i and diagnosis j at least once."""
    grouped = (
        patient_path
        .group_by(group_col)
        .agg(pl.col("diag_code").unique())
    )

    counts = Counter()
    for diag_list in grouped["diag_code"]:
        if len(diag_list) < 2:
            continue
        for d1, d2 in combinations(sorted(diag_list), 2):
            counts[(d1, d2)] += 1

    return counts


def _diagnosis_prevalence(patient_path: pl.DataFrame) -> dict:
    """P_i: number of distinct PATIENTS who ever have diagnosis i — always
    counted per patient, even inside a CONSULT-level graph, since P_i and
    N are defined as individuals, not encounters."""
    prevalence = (
        patient_path
        .select("patient_id", "diag_code")
        .unique()
        .group_by("diag_code")
        .agg(pl.len().alias("n_patients"))
    )
    return dict(zip(prevalence["diag_code"], prevalence["n_patients"]))


def _graph_from_weights(weights: dict) -> nx.Graph:
    G = nx.Graph()
    for (d1, d2), w in weights.items():
        G.add_edge(d1, d2, weight=w)
    return G


def build_graph_from(
    patient_path: pl.DataFrame,
    type_graph: TypeEnum = TypeEnum.PATIENT,
    weight_enum: WeightEnum = WeightEnum.CIJ,
    cache_path: str | None = None,
) -> nx.Graph:

    group_col = "patient_id" if type_graph == TypeEnum.PATIENT else "consulta_id"
    counts = _pairwise_cooccurrence_counts(patient_path, group_col)

    if weight_enum == WeightEnum.CIJ:
        # CIJ_ij = C_ij / N
        n_patients = patient_path.select("patient_id").n_unique()
        weights = {pair: c / n_patients for pair, c in counts.items()}

    elif weight_enum == WeightEnum.SCI:
        # SCI_ij = C_ij / sqrt(P_i * P_j)
        prevalence = _diagnosis_prevalence(patient_path)
        weights = {
            (d1, d2): c / math.sqrt(prevalence[d1] * prevalence[d2])
            for (d1, d2), c in counts.items()
        }

    else:
        raise ValueError(f"Unhandled weight_enum: {weight_enum}")

    G = _graph_from_weights(weights)

    if cache_path is not None:
        nx.write_gml(G, cache_path)

    return G


# --------------------------------------------------------------------------
# /graph route logic
# --------------------------------------------------------------------------

SUPPORTED_DOWNLOAD_FORMATS = {"gml"}  # graphml/gexf/csv land here later


def _cache_key(graphfilters: GraphFilters) -> str:
    """Hash of everything that determines the graph itself — deliberately
    excludes output format, so GML and JSON requests for the same query
    share one key instead of each triggering their own rebuild."""
    payload = json.dumps(graphfilters.model_dump(mode="json"), sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


async def get_graph(db, graphfilters: GraphFilters, download_format: str | None = None) -> dict:
    # patients_path needs the whole GraphFilters (it reads both
    # .filters and .remove_diags), not just the inner CommonFilters.
    path_df = await patients_path(db, graphfilters)
    patient_count = path_df.select("patient_id").n_unique()

    if patient_count < MIN_PATIENT_COUNT:
        logger.warning(
            "graph request refused: cell too small",
            extra={"patient_count": patient_count, "filters": graphfilters.model_dump(mode="json")},
        )
        raise ValueError(f"Query too narrow: only {patient_count} matching patients")

    cache_key = _cache_key(graphfilters)

    # --- download path: build once, reuse the cached file on repeat requests ---
    if download_format is not None:
        if download_format not in SUPPORTED_DOWNLOAD_FORMATS:
            raise ValueError(f"Unsupported download_format: {download_format!r}")

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_path = CACHE_DIR / f"{cache_key}.{download_format}"

        if cache_path.exists():
            logger.info("graph cache hit", extra={"cache_key": cache_key, "format": download_format})
        else:
            G = build_graph_from(
                path_df,
                type_graph=graphfilters.type_network,
                weight_enum=graphfilters.edge_weight_model,
                cache_path=str(cache_path),
            )
            logger.info(
                "graph built",
                extra={
                    "cache_key": cache_key,
                    "format": download_format,
                    "patient_count": patient_count,
                    "node_count": G.number_of_nodes(),
                    "edge_count": G.number_of_edges(),
                    "filters": graphfilters.model_dump(mode="json"),
                },
            )

        del path_df
        return {"file_path": str(cache_path), "patient_count": patient_count}

    # --- frontend path: build in memory, hand back JSON, no disk cache (Redis later) ---
    G = build_graph_from(
        path_df,
        type_graph=graphfilters.type_network,
        weight_enum=graphfilters.edge_weight_model,
    )

    logger.info(
        "graph built",
        extra={
            "cache_key": cache_key,
            "format": "json",
            "patient_count": patient_count,
            "node_count": G.number_of_nodes(),
            "edge_count": G.number_of_edges(),
            "filters": graphfilters.model_dump(mode="json"),
        },
    )

    del path_df
    return {"graph": nx.node_link_data(G, edges="links"), "patient_count": patient_count}
