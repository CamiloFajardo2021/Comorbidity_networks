import polars as pl
import os
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from logs_ import LogsMongoDB,LogsFormat,LogStatus
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
# Per-run parameters — from the command line, not the environment
# usage: docker compose run etl 17001 2024
# ------------------------


parser = argparse.ArgumentParser()
parser.add_argument("--anio", required=True, help="Year to process, e.g. 2024")
parser.add_argument(
    "--municipios", required=True, nargs="+",
    help="One or more MunicipioCD values, e.g. --municipios 17001 or --municipios 17001 25307",
)
args = parser.parse_args()

ANIO = args.anio
MUNICIPIO = args.municipios   # always a list from here on, length 1 or more


# ------------------------
# Raw file paths — same naming convention as etl_ingest.py
# ------------------------
filepath_rips = DATA_DIR / f"RIPS_{ANIO}.txt"
filepath_bdua = DATA_DIR / f"BDUA_{ANIO}_{int(ANIO) + 2}.txt" #2014-2016 , 2017-2019 , 2019-2023
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
            .filter(pl.col("MunicipioCD") == mun)
            .sink_parquet(outdir / "data.parquet")
        )
        logger.info(f"Parquet save successfull in {outdir}")
    except Exception as e:
        logger.error(f"Error to save parquet")



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
        pl.col("FECHA_CONSUL")
        .str.replace_all("-", "")
        .str.slice(0, 8)
        .alias("FECHA_CONSUL").cast(pl.Int64)
    )

    

    return


# ------------------------
# Main process : RIPS + BDUA + Diag_rels joins
# ------------------------

def get_df_final(municipio, db):
    global diag_rel

    rips = pl.scan_parquet(
        str(PARQUET_DIR / f"municipio={municipio}" / "*.parquet")
    )

    

    rips = rips.rename({"MunicipioCD":"MunicipioConsulta"})
    rips_2024 = rips.join(
        bdua_g_2024,
        on='PersonaID',
        how="left"
)

    rips_2024 = rips_2024.with_columns(

        pl.col("TipoEventoRIPSDesc")
        .str.to_uppercase()
        .str.slice(0, 1)
        .alias("tipo_letter")
    )

    diag_rel = diag_rel.with_columns(

        pl.col("TipoAtencion")
        .str.to_uppercase()
        .str.slice(0, 1)
        .alias("tipo_letter")
    )

    df = rips_2024.join(

        diag_rel,

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

            pl.col("FinalidadConsultaCD").
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

            pl.col("DescNuevoRegistro")
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

        ).alias("_id")
    )

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
            collection.insert_many(docs, ordered=False)

        except Exception as e:
            log_error = LogsFormat(
                municipio=municipio, anio=ANIO,
                status=LogStatus.ERROR,
                error=str(e)[:50], batch_int=i,
            )
            logs.write_log(log_error)
            break

    else:
        logs.write_log(LogsFormat(municipio=municipio, anio=ANIO, status=LogStatus.PROCESSED))
        logger.info(f"Batch number process : {i}")



    
    logger.info(f"Municipio {municipio}_{ANIO} cargado exitosamente")

    return

#input args
#municipio : str or list[str]
#ANIO
if __name__ == "__main__":

    client = MongoClient(os.environ["MONGO_URI"])
    db = client["rips_db"]
    logs = LogsMongoDB(db)

    bdua_and_diag_rel_load(ANIO)

    for municipio in MUNICIPIO:
        if logs.get_status(municipio, ANIO) == LogStatus.PROCESSED:
            continue

        parquet_pre(municipio)
        get_df_final(municipio, db)

    client.close()