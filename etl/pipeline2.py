import polars as pl
import os
import re
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from logs_ import LogsMongoDB,LogsFormat,LogStatus,Binnacle
import os
import argparse
from pathlib import Path
import polars as pl
import logging

# ------------------------
# Deployment config — from the container's environment (docker-compose.yaml)
# ------------------------
DATA_DIR = Path(os.environ["DATA_PATH"])       # /data — the read-only mount of the external drive
PARQUET_DIR = Path(os.environ["PARQUET_PATH"]) # /parquet


# ------------------------
# MongoDB hard-caps a single document at 16MB (16777216 bytes). A handful
# of high-utilization patients can end up with a 'consultas' array large
# enough to cross that line — MONGO_MAX_DOC_BYTES is the real limit,
# MAX_SAFE_DOC_BYTES leaves headroom so BSON overhead we don't account for
# exactly doesn't push us back over it.
# ------------------------
MONGO_MAX_DOC_BYTES = 16 * 1024 * 1024
DOC_SIZE_SAFETY_MARGIN = 2 * 1024 * 1024
MAX_SAFE_DOC_BYTES = MONGO_MAX_DOC_BYTES - DOC_SIZE_SAFETY_MARGIN

# ------------------------
# Per-run parameters — from the command line, not the environment
# usage: docker compose run etl 17001 2024
# ------------------------


parser = argparse.ArgumentParser()
parser.add_argument("--anio", required=True, help="Year to process, e.g. 2024")
parser.add_argument(
    "--municipios", required=True, nargs="+",
    help="One or more MunicipioCD values, e.g. --municipios 17001 or --municipios 17001 25307",
)
parser.add_argument(
    "--batch-mode", choices=["auto", "always", "never"], default="auto",
    help=(
        "'auto' (default) decides per municipio from its distinct-patient count; "
        "'always' forces batched processing; 'never' forces the non-batched path."
    ),
)
parser.add_argument(
    "--batch-threshold", type=int, default=500_000,
    help=(
        "Distinct-patient count above which 'auto' mode switches a municipio "
        "to batched processing. 50_000 is a placeholder -- tune it empirically "
        "(e.g. via .profile()) for your hardware."
    ),
)
args = parser.parse_args()

ANIO = args.anio
MUNICIPIO = args.municipios   # always a list from here on, length 1 or more


# ------------------------
# Raw file paths — same naming convention as etl_ingest.py
# ------------------------
filepath_rips = DATA_DIR / f"RIPS_{ANIO}.txt"
#filepath_bdua = DATA_DIR / f"BDUA_{ANIO}_{int(ANIO) + 2}.txt" #2014-2016 , 2017-2020 , 2021-2024
if int(ANIO) in (2014,2015,2016):
    filepath_bdua = DATA_DIR / f"BDUA_2014_2016.txt"
elif int(ANIO) in (2017,2018,2019,2020):
    filepath_bdua = DATA_DIR / f"BDUA_2017_2020.txt"
elif int(ANIO) in (2021,2022,2023,2024):
    filepath_bdua = DATA_DIR / f"BDUA_2021_2024.txt"
else:
    raise ValueError("EL año debe estar entre 2014 a 2024")

path_diag_rels = DATA_DIR / f"UNAL_{ANIO}.txt"   # confirm this matches the real filename on the drive

# ------------------------
# LOG settings
# ------------------------

# ------------------------
# LOG settings
# ------------------------
import logging
from logging.handlers import RotatingFileHandler
import sys

LOG_DIR = Path(os.environ["LOG_PATH"])  # /logs — bind-mounted, so logs survive container restarts
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("etl.pipeline2")
logger.setLevel(logging.INFO)

if not logger.handlers:  # guard against duplicate handlers if this module ever gets imported twice
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # One file per run (anio + municipios), so separate `docker compose run` invocations
    # don't interleave into a single giant log.
    municipios_tag = "-".join(MUNICIPIO)
    log_file = LOG_DIR / f"pipeline2_{ANIO}_{municipios_tag}.log"

    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Mirror to stdout too, so `docker compose logs etl` still shows it live
    # (RotatingFileHandler alone would only write to /logs).
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


def _log_uncaught_exceptions(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = _log_uncaught_exceptions

# ------------------------
# Safety net for MongoDB's 16MB/doc limit
# ------------------------
def _doc_size(doc: dict) -> int:
    try:
        return len(BSON.encode(doc))
    except Exception:
        # If it doesn't even encode, let insert_many surface the real error.
        return -1
 
 
def _chunks_that_fit(base: dict, items: list, max_bytes: int) -> list[list]:
    """Recursively halve `items` until every chunk, combined with `base`,
    fits under max_bytes. Shared by split_oversized_doc and by
    enforce_doc_size_limits (batched path) so both use the same slicing
    logic."""
    if not items:
        return []
    candidate_size = _doc_size({**base, "consultas": items})
    if len(items) == 1 or (candidate_size != -1 and candidate_size <= max_bytes):
        return [items]
    mid = len(items) // 2
    return _chunks_that_fit(base, items[:mid], max_bytes) + _chunks_that_fit(base, items[mid:], max_bytes)


def split_oversized_doc(doc: dict, max_bytes: int = MAX_SAFE_DOC_BYTES, part_start: int = 1) -> list[dict]:
    """
    A patient document is one row per PersonaID with a 'consultas' array
    holding every consultation/procedure for that patient in that year.
    For the overwhelming majority of patients that's small, but a data
    issue upstream (duplicated source rows, a join fan-out — see the
    dedup added in get_df_final) can leave a handful of patients with a
    'consultas' array big enough to push the whole document past Mongo's
    16MB cap.

    Rather than let that document take the whole batch (and, before this
    fix, the whole remaining run) down, split it into multiple documents
    that each fit, e.g. "1234567_2024" -> "1234567_2024_part1",
    "..._part2", ... Patient-level fields are duplicated on every part;
    only 'consultas' is sliced. This never drops data — worst case, a
    downstream reader that only fetches _id == "{PersonaID}_{anio}" needs
    to also glob "{PersonaID}_{anio}_part*" to see the rest.

    part_start lets a caller that's re-splitting an already-partially-split
    patient (see enforce_doc_size_limits, used by the batched $push path in
    get_df_final_batches) continue numbering instead of colliding with
    _partN docs that already exist for this patient.
    """
    size = _doc_size(doc)
    if size == -1 or size <= max_bytes:
        return [doc]

    consultas = doc.get("consultas") or []
    if len(consultas) <= 1:
        # Nothing left to split on — not the array that's causing it.
        return [doc]

    base = {k: v for k, v in doc.items() if k != "consultas"}
    base_id = doc["_id"]

    parts = _chunks_that_fit(base, consultas, max_bytes)
    return [
        {**base, "consultas": part, "_id": f"{base_id}_part{i}"}
        for i, part in enumerate(parts, start=part_start)
    ]


_PART_RE = re.compile(r"_part(\d+)$")


def _next_part_number(collection, base_id: str) -> int:
    """Highest existing _partN suffix already stored for this patient, so
    re-splitting a doc that's been split before doesn't overwrite parts
    created by an earlier split."""
    cursor = collection.find(
        {"_id": {"$regex": f"^{re.escape(base_id)}_part\\d+$"}},
        {"_id": 1},
    )
    max_n = 0
    for d in cursor:
        m = _PART_RE.search(d["_id"])
        if m:
            max_n = max(max_n, int(m.group(1)))
    return max_n


def enforce_doc_size_limits(collection, ids: list, max_bytes: int = MAX_SAFE_DOC_BYTES) -> list:
    """
    Post-$push safety net for the batched path (get_df_final_batches).

    get_df_final can size-check a patient doc before insert because it
    builds the whole document in memory first. get_df_final_batches instead
    grows each patient's 'consultas' array incrementally via $push/upsert
    across many chunks (and possibly many runs), so there's no single point
    to pre-check size the same way. Instead, after each bulk_write, this
    cheaply re-checks the size of just the docs that chunk touched (via
    $bsonSize, which avoids pulling full documents over the wire) and, for
    any that crossed max_bytes, splits them with split_oversized_doc —
    mirroring the same belt-and-suspenders check already done in
    get_df_final.

    The bare _id is abandoned (deleted) in favor of one or more _id_partN
    docs, consistent with how split_oversized_doc already documents
    itself. A later chunk that $push'es new consultas for that patient will
    simply recreate a fresh doc at the bare _id via upsert, which will
    itself be split again if it grows too far.
    """
    if not ids:
        return []

    sizes = collection.aggregate([
        {"$match": {"_id": {"$in": ids}}},
        {"$project": {"size": {"$bsonSize": "$$ROOT"}}},
    ])
    oversized_ids = [d["_id"] for d in sizes if d["size"] > max_bytes]
    if not oversized_ids:
        return []

    for _id in oversized_ids:
        doc = collection.find_one({"_id": _id})
        if doc is None:
            continue  # already handled/raced; next check will catch it if it recurs

        part_start = _next_part_number(collection, _id) + 1
        parts = split_oversized_doc(doc, max_bytes=max_bytes, part_start=part_start)
        if len(parts) <= 1:
            continue  # single oversized consulta -- nothing more we can do

        collection.delete_one({"_id": _id})
        collection.insert_many(parts, ordered=False)

    print(
        f"[warn] {len(oversized_ids)} patient doc(s) exceeded the safe size "
        f"after $push and were split into _partN docs: {oversized_ids}"
    )
    return oversized_ids


# ------------------------
# Filter big raw file RIPS-year for municipio and save as parquet
# ------------------------
def parquet_pre(mun: str):
    logger.info("Parquet creation")
    outdir = PARQUET_DIR / f"municipio={mun}"
    outdir.mkdir(parents=True, exist_ok=True)

    try:
        (
            pl.scan_csv(filepath_rips, separator='|',
                        encoding="utf8-lossy",
                        ignore_errors=True,
                        infer_schema_length=10000,
                        null_values=["", "NULL", "null", "NA", "N/A", "."],
                        truncate_ragged_lines=True)
            .filter(pl.col("MunicipioCD") == int(mun))
            .sink_parquet(outdir / "data.parquet")
        )
        logger.info(f"Parquet save successfull in {outdir}")
    except Exception as e:
        logger.error(f"Error to save parquet {e}")



# ------------------------
# Load globally bdua and diag_rel
# ------------------------
def bdua_and_diag_rel_load(ANIO :str):

    global bdua_g_2024
    global diag_rel
    bdua = (
        pl.scan_csv(
            filepath_bdua,
            separator="|",
            encoding="utf8-lossy",
            truncate_ragged_lines=True
        )
        .filter(
            pl.col("FechaCorteID")
            .cast(pl.Utf8)
            .str.contains(ANIO)
        )
    )

    bdua_2014 = bdua.collect()

    grouped = (
        bdua_2014
        .group_by("PersonaID")
        .agg([
            pl.first("SexoDesc"),

            pl.first("MunicipioCD"),

            pl.first("DepartamentoCD"),

            pl.first("zona"),

            pl.first("DescAntigRegistro"),

            pl.first("ActividadEconomicaDesc"),

            pl.first("EtniaDesc"),

            pl.col("DescNuevoRegistro")
            .unique()
            .sort()
            .alias("discapacidad"),

            pl.first("FechaCorteID")
        ])
    )


    bdua_g_2024 = grouped.lazy()



    diag_rel = pl.scan_csv(
        path_diag_rels,
        separator='|',
        encoding="utf8-lossy",

        ignore_errors=True,

        truncate_ragged_lines=True,

        dtypes={
            "año": pl.Utf8
        }
    )

    diag_rel = diag_rel.with_columns(
        pl.when(
            pl.col("FECHA_CONSUL")
            .str.replace_all("-", "")
            .str.slice(0, 8)
            .str.contains(r"^\d{8}$")
        )
        .then(
            pl.col("FECHA_CONSUL")
            .str.replace_all("-", "")
            .str.slice(0, 8)
            .cast(pl.Int64)
        )
        .otherwise(None)
        .alias("FECHA_CONSUL")
    )

    

    return


# ------------------------
# Main process : RIPS + BDUA + Diag_rels joins
# ------------------------

def bdua_for_municipio(municipio):
    ids = (
        pl.scan_parquet(str(PARQUET_DIR / f"municipio={municipio}" / "*.parquet"))
        .select("PersonaID")
        .unique()
        .collect()
    )

    logger.info("Filtering BDUA")

    d_rel = (diag_rel.join(ids.lazy(), left_on="PersonaBasicaID", right_on="PersonaID", how="semi").collect(streaming=True).lazy())
    bdua_rel = (
        bdua_g_2024
        .join(ids.lazy(), on="PersonaID", how="semi")
        .collect(streaming=True)   # pin it in memory once
        .lazy()
    )
    
    return d_rel, bdua_rel, ids.height


def get_df_final(municipio, db, diag_rel_pre, bdua_g_2024_pre):
    #global diag_rel

    rips = pl.scan_parquet(
        str(PARQUET_DIR / f"municipio={municipio}" / "*.parquet")
    )

    

    rips = rips.rename({"MunicipioCD":"MunicipioConsulta"})
    rips_2024 = rips.join(
        bdua_g_2024_pre,
        on='PersonaID',
        how="left"
)

    rips_2024 = rips_2024.with_columns(

        pl.col("TipoEventoRIPSDesc")
        .str.to_uppercase()
        .str.slice(0, 1)
        .alias("tipo_letter")
    )

    diag_rel2 = diag_rel_pre.with_columns(

        pl.col("TipoAtencion")
        .str.to_uppercase()
        .str.slice(0, 1)
        .alias("tipo_letter")
    )

    df = rips_2024.join(

        diag_rel2,

        left_on=[
            "PersonaID",
            "fechaid",
            "DxPrincipal",
            "tipo_letter"
        ],

        right_on=[
            "PersonaBasicaID",
            "FECHA_CONSUL",
            "COD_DIAG_PRIN",
            "tipo_letter"
        ],

        how="left"
    )

    df = df.unique() #avoid exact duplicates

    df = df.with_columns([

        pl.when(
            pl.col("COD_DIAG_R1")
            .str.to_lowercase()
            .eq("null")
        )
        .then(None)
        .otherwise(pl.col("COD_DIAG_R1"))
        .alias("COD_DIAG_R1"),

        pl.when(
            pl.col("COD_DIAG_R2")
            .str.to_lowercase()
            .eq("null")
        )
        .then(None)
        .otherwise(pl.col("COD_DIAG_R2"))
        .alias("COD_DIAG_R2"),

        pl.when(
            pl.col("COD_DIAG_R3")
            .str.to_lowercase()
            .eq("null")
        )
        .then(None)
        .otherwise(pl.col("COD_DIAG_R3"))
        .alias("COD_DIAG_R3")
    ])

    matches = [x for x in df.columns if x.lower() == "finalidadconsultacd"] #change df.columns
    assert len(matches) == 1, f"expected exactly one FinalidadConsultaCD column, found {matches}"
    finalidad_consulta_header = matches[0]

    df = df.with_columns(

        pl.struct([

            pl.col("fechaid")
            .cast(pl.Utf8)
            .str.strptime(
                pl.Datetime,
                "%Y%m%d"
            )
            .alias("fecha"),

            pl.col("TipoEventoRIPSDesc").str.strip_chars().str.to_uppercase().str.slice(0,1)
            .alias("tipo_evento"),

            pl.col("DxPrincipal")
            .alias("diag_prin"),

            pl.concat_list([
                "COD_DIAG_R1",
                "COD_DIAG_R2",
                "COD_DIAG_R3"
            ])
            .list.drop_nulls()
            .alias("diag_rel"),

            pl.col("Prestador")
            .alias("prestador"),

            pl.col("TipoAtencion").str.strip_chars().str.to_uppercase().str.slice(0,1)
            .alias("tipo_atencion"),

            pl.col("CostoConsulta")
            .alias("costo_consulta"),

            pl.col("CostoProcedimiento")
            .alias("costo_procedimiento"),

            pl.col("CodigoProcedimiento")
            .alias("codigo_procedimiento"),

            pl.col("NumeroDiasEstancia")
            .alias("dias_estancia"),

            pl.col("TipoDiagnosticoPrincipalCD").
            alias("tipo_diagnostico"),

            pl.col(finalidad_consulta_header).
            alias("finalidad_consulta"),

            pl.col("Prestador").
            alias("Prestador")

        ]).alias("consulta")
    )

    patients = (

        df
        .sort("fechaid")

        .group_by("PersonaID")

        .agg([

            pl.first("SexoDesc").str.strip_chars().str.to_uppercase().str.slice(0, 1).alias("sexo"),

            pl.first("edad"), #a corte fecha consulta

            pl.first("TipoUsuarioCD").alias("Regimen"),

            pl.first("MunicipioCD") #municipio de afiliacion -- new
            .alias("municipioAfiliacion"),

            pl.first("MunicipioConsulta") #municipio donde ocurre la consulta -- new
            .alias("municipioConsulta"),

            pl.first("DepartamentoCD")
            .alias("departamento"),

            pl.first("zona"),

            pl.col("año").drop_nulls().first().alias("anio"),

            pl.first("EtniaDesc")
            .alias("etnia"),

            pl.first("ActividadEconomicaDesc")
            .alias("actividad_economica"),

            pl.col("discapacidad")
            .drop_nulls()
            .first(),

            pl.col("DescAntigRegistro")
            .drop_nulls()
            .first(),

            pl.col("consulta")
            .alias("consultas")
        ])
    )

    año = ANIO

    patients = patients.with_columns(

        pl.col("anio")
        .fill_null(año)
    )

    patients = patients.with_columns(

        (
            pl.col("PersonaID")
            .cast(pl.Utf8)

            + "_"

            + pl.col("anio")
                .cast(pl.Utf8)

            + "_"

            + pl.col("municipioAfiliacion")
                .cast(pl.Utf8)

        ).alias("_id")
    )

    patients = patients.sort("PersonaID")  # deterministic order so batch_start/iter_slices resume the same patients across reruns

    patients_df = patients.collect(streaming=True)

  

    collection = db["patients"]


    #collection.delete_many({}) # if the run failed so it would not be problems with the id



    BATCH = 1000  # fixed

    logger.info(f"BACH SIZE : {BATCH}")

    if logs.get_status(municipio, ANIO) == LogStatus.ERROR:
        batch_start = logs.get_batch_int(municipio, ANIO)
    else:
        batch_start = 0

    for i, batch in enumerate(patients_df.iter_slices(BATCH)):

        if i < batch_start:
            continue

        try:
            batch = batch.with_columns(
                pl.col(pl.Float64).fill_nan(None),
                pl.col(pl.Float32).fill_nan(None),
            )
            docs = batch.to_dicts()

            # Belt-and-suspenders: even after the dedup above, split any
            # doc that's still over the safe size instead of letting it
            # take out the batch.
            safe_docs = []
            oversized_ids = []
            for d in docs:
                parts = split_oversized_doc(d)
                if len(parts) > 1:
                    oversized_ids.append(d["_id"])
                safe_docs.extend(parts)
 
            if oversized_ids:
                print(
                    f"[warn] municipio={municipio} anio={ANIO} batch={i}: "
                    f"{len(oversized_ids)} patient doc(s) exceeded the safe "
                    f"size and were split into _partN docs: {oversized_ids}"
                )



            collection.insert_many(safe_docs, ordered=False)

        except Exception as e:
            log_error = LogsFormat(
                municipio=municipio, anio=ANIO,
                status=LogStatus.ERROR,
                error=str(e)[:50], batch_int=i,
            )
            logs.write_log(log_error)
            raise

    else:
        logs.write_log(LogsFormat(municipio=municipio, anio=ANIO, status=LogStatus.PROCESSED))
        logger.info(f"Batch number process : {i}")



    
    logger.info(f"Municipio {municipio}_{ANIO} cargado exitosamente")

    Binnacle().insert_(municipio=municipio,anio=ANIO)

    return

#===================================================================
# Batch read
#===================================================================
N_ROWS = 10000

from pymongo import UpdateOne

from itertools import islice


def _chunked(iterable, size):
    """Yield successive `size`-length lists from `iterable` (local stand-in
    for more_itertools.chunked, which isn't pinned in requirements.txt)."""
    it = iter(iterable)
    while chunk := list(islice(it, size)):
        yield chunk


import hashlib

# Fields that jointly identify a distinct consulta -- deliberately everything
# in the struct except the duplicate `Prestador`/`prestador` pair. Used to
# de-duplicate before $push'ing into a patient's `consultas` array (see
# get_df_final_batches below): a retried chunk after a partial bulk_write
# failure can otherwise push the same consulta twice, since ordered=False
# doesn't tell us which ops in a failed chunk already applied.
_CONSULTA_HASH_FIELDS = (
    "fecha", "tipo_evento", "diag_prin", "diag_rel", "prestador",
    "tipo_atencion", "costo_consulta", "costo_procedimiento",
    "codigo_procedimiento", "dias_estancia", "tipo_diagnostico",
    "finalidad_consulta",
)


def _consulta_hash(c: dict) -> str:
    parts = []
    for field in _CONSULTA_HASH_FIELDS:
        value = c.get(field)
        if isinstance(value, list):
            value = sorted(value)
        parts.append(str(value))
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def get_df_final_batches(municipio, db, diag_rel_pre, bdua_g_2024_pre, i_0=0):
    diag_rel_batch = diag_rel_pre.with_columns(
        pl.col("TipoAtencion").str.to_uppercase().str.slice(0, 1).alias("tipo_letter")
    )

    rips = (
        pl.scan_parquet(str(PARQUET_DIR / f"municipio={municipio}" / "*.parquet"))
        .slice(i_0, N_ROWS)
        .rename({"MunicipioCD": "MunicipioConsulta"})
    )

    rips_2024 = rips.join(bdua_g_2024_pre, on="PersonaID", how="left").with_columns(
        pl.col("TipoEventoRIPSDesc").str.to_uppercase().str.slice(0, 1).alias("tipo_letter")
    )

    df = rips_2024.join(
        diag_rel_batch,
        left_on=["PersonaID", "fechaid", "DxPrincipal", "tipo_letter"],
        right_on=["PersonaBasicaID", "FECHA_CONSUL", "COD_DIAG_PRIN", "tipo_letter"],
        how="left",
    )

    df = df.unique()

    # ... COD_DIAG_R1/R2/R3 null cleanup and the `consulta` struct build, unchanged ...

    df = df.with_columns([
    
        pl.when(
            pl.col("COD_DIAG_R1")
            .str.to_lowercase()
            .eq("null")
        )
        .then(None)
        .otherwise(pl.col("COD_DIAG_R1"))
        .alias("COD_DIAG_R1"),

        pl.when(
            pl.col("COD_DIAG_R2")
            .str.to_lowercase()
            .eq("null")
        )
        .then(None)
        .otherwise(pl.col("COD_DIAG_R2"))
        .alias("COD_DIAG_R2"),

        pl.when(
            pl.col("COD_DIAG_R3")
            .str.to_lowercase()
            .eq("null")
        )
        .then(None)
        .otherwise(pl.col("COD_DIAG_R3"))
        .alias("COD_DIAG_R3")
    ])

    matches = [x for x in df.columns if x.lower() == "finalidadconsultacd"] #change df.columns
    assert len(matches) == 1, f"expected exactly one FinalidadConsultaCD column, found {matches}"
    finalidad_consulta_header = matches[0]

    df = df.with_columns(

        pl.struct([

            pl.col("fechaid")
            .cast(pl.Utf8)
            .str.strptime(
                pl.Datetime,
                "%Y%m%d"
            )
            .alias("fecha"),

            pl.col("TipoEventoRIPSDesc").str.strip_chars().str.to_uppercase().str.slice(0,1)
            .alias("tipo_evento"),

            pl.col("DxPrincipal")
            .alias("diag_prin"),

            pl.concat_list([
                "COD_DIAG_R1",
                "COD_DIAG_R2",
                "COD_DIAG_R3"
            ])
            .list.drop_nulls()
            .alias("diag_rel"),

            pl.col("Prestador")
            .alias("prestador"),

            pl.col("TipoAtencion").str.strip_chars().str.to_uppercase().str.slice(0,1)
            .alias("tipo_atencion"),

            pl.col("CostoConsulta")
            .alias("costo_consulta"),

            pl.col("CostoProcedimiento")
            .alias("costo_procedimiento"),

            pl.col("CodigoProcedimiento")
            .alias("codigo_procedimiento"),

            pl.col("NumeroDiasEstancia")
            .alias("dias_estancia"),

            pl.col("TipoDiagnosticoPrincipalCD").
            alias("tipo_diagnostico"),

            pl.col(finalidad_consulta_header).
            alias("finalidad_consulta"),

            pl.col("Prestador").
            alias("Prestador")

        ]).alias("consulta")
    )

    patients = (

        df

        .group_by("PersonaID")

        .agg([

            pl.first("SexoDesc").str.strip_chars().str.to_uppercase().str.slice(0, 1).alias("sexo"),

            pl.first("edad"), #a corte fecha consulta

            pl.first("TipoUsuarioCD").alias("Regimen"),

            pl.first("MunicipioCD") #municipio de afiliacion -- new
            .alias("municipioAfiliacion"),

            pl.first("MunicipioConsulta") #municipio donde ocurre la consulta -- new
            .alias("municipioConsulta"),

            pl.first("DepartamentoCD")
            .alias("departamento"),

            pl.first("zona"),

            pl.col("año").drop_nulls().first().alias("anio"),

            pl.first("EtniaDesc")
            .alias("etnia"),

            pl.first("ActividadEconomicaDesc")
            .alias("actividad_economica"),

            pl.col("discapacidad")
            .drop_nulls()
            .first(),

            pl.col("DescAntigRegistro")
            .drop_nulls()
            .first(),

            pl.col("consulta")
            .alias("consultas")
        ])
    )

    año = ANIO

    patients = patients.with_columns(
    
        pl.col("anio")
        .fill_null(año)
    )

    patients = patients.with_columns(

        (
            pl.col("PersonaID")
            .cast(pl.Utf8)

            + "_"

            + pl.col("anio")
                .cast(pl.Utf8)

            + "_"

            + pl.col("municipioAfiliacion")
                .cast(pl.Utf8)

        ).alias("_id")
    )



    patients_df = patients.collect(streaming=True)
    if patients_df.height == 0:
        return 0    # signals "past end of file" to the caller

    docs = patients_df.with_columns(
        pl.col(pl.Float64).fill_nan(None),
        pl.col(pl.Float32).fill_nan(None),
    ).to_dicts()

    for doc in docs:
        for c in doc["consultas"]:
            c["hash"] = _consulta_hash(c)

    collection = db["patients"]

    # One round trip for the whole batch (not per patient): pull back
    # whichever hashes are already stored for the patients in this batch, so
    # a retry never pushes a consulta that's already in the array -- this is
    # the real fix for the ordered=False partial-retry duplication risk, the
    # batch_int/inner_batch_index resume logic below is now just an
    # optimization to skip *redoing* work, not a correctness requirement.
    ids_in_batch = [doc["_id"] for doc in docs]
    existing_hashes = {
        d["_id"]: {c["hash"] for c in d.get("consultas", []) if "hash" in c}
        for d in collection.find({"_id": {"$in": ids_in_batch}}, {"consultas.hash": 1})
    }

    ops = []
    op_ids = []  # parallel to ops -- which _id each op targets, for enforce_doc_size_limits
    for doc in docs:
        _id = doc.pop("_id")
        consultas = doc.pop("consultas")
        already = existing_hashes.get(_id, set())
        new_consultas = [c for c in consultas if c["hash"] not in already]
        if not new_consultas and _id in existing_hashes:
            continue  # nothing new for a patient we've already fully recorded
        ops.append(
            UpdateOne(
                {"_id": _id},
                {
                    "$setOnInsert": doc,
                    "$push": {"consultas": {"$each": new_consultas, "$sort": {"fecha": 1}}},
                },
                upsert=True,
            )
        )
        op_ids.append(_id)

    current_batch_int = i_0 // N_ROWS

    # Resume support: if the last thing logged for this municipio+anio was an
    # ERROR for this same outer batch, pick up at the chunk that failed
    # instead of redoing chunks that already committed. (ordered=False
    # bulk_write doesn't tell us which ops inside a failed chunk applied
    # before the error, so a retried chunk can still double-push a few docs
    # -- a much smaller risk than silently skipping data.)
    last_status, last_batch_int, last_inner_index = logs.get_progress(municipio, ANIO)
    if last_status == LogStatus.ERROR and last_batch_int == current_batch_int:
        resume_from = last_inner_index or 0
    else:
        resume_from = 0

    for j, (chunk, chunk_ids) in enumerate(zip(_chunked(ops, 1000), _chunked(op_ids, 1000))):
        if j < resume_from:
            continue

        try:
            collection.bulk_write(chunk, ordered=False)
            enforce_doc_size_limits(collection, chunk_ids)
            logs.write_log(LogsFormat(
                municipio=municipio, anio=ANIO, status=LogStatus.PROCESSED,
                batch_int=current_batch_int, inner_batch_index=j,
            ))
        except Exception as e:
            logs.write_log(LogsFormat(
                municipio=municipio, anio=ANIO, status=LogStatus.ERROR,
                error=str(e)[:50], batch_int=current_batch_int, inner_batch_index=j,
            ))
            raise

    return patients_df.height

#input args
#municipio : str or list[str]
#ANIO
if __name__ == "__main__":

    logger.info("Iniciando")

    client = MongoClient(os.environ["MONGO_URI"])
    db = client["rips_db"]
    logs = LogsMongoDB(db)

    logger.info("Cargando BDUA y DIAG_REL")
    bdua_and_diag_rel_load(ANIO)

    for municipio in MUNICIPIO:
        if logs.is_fully_processed(municipio, ANIO):
            logger.info("Procesado")
            continue

        parquet_pre(municipio)
        diag_rel_pre, bdua_g_2024_pre, n_patients = bdua_for_municipio(municipio=municipio)

        use_batch = (
            args.batch_mode == "always"
            or (args.batch_mode == "auto" and n_patients > args.batch_threshold)
        )

        if not use_batch:
            get_df_final(municipio, db, diag_rel_pre, bdua_g_2024_pre)
        else:
            last_status, last_batch_int, _ = logs.get_progress(municipio, ANIO)
            i_0 = (
                last_batch_int * N_ROWS
                if last_status == LogStatus.ERROR and last_batch_int is not None
                else 0
            )

            batch_num = i_0 // N_ROWS
            total_rows = 0
            while (n := get_df_final_batches(municipio, db, diag_rel_pre, bdua_g_2024_pre, i_0)) > 0:
                total_rows += n
                logger.info(
                    f"[{municipio}] batch {batch_num} (i_0={i_0}) -> {n} patient rows written "
                    f"(running total: {total_rows})"
                )
                batch_num += 1
                i_0 += N_ROWS

            logs.write_log(LogsFormat(municipio=municipio, anio=ANIO, status=LogStatus.PROCESSED))
            Binnacle().insert_(municipio=municipio, anio=ANIO)

    client.close()
