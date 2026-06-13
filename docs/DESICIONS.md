`13/06/2026`

Initial 

**Database**
 1. MongoDB docker container
 2. Collections : Patients (unique by patient and year) - ingest_logs (for each year and municipio) - graph_cache
 3. Indexes : On socioeconomics variables `sexo`,`regimen`,`zona`,`discapacidad`,`etnia`,`grupo etario`
 4. Optimizations : To review

**Backend - API**
 1. Pymongo - Python - FastAPI
 2. API GET : diagnostics for population
 3. API POST : information of population (for example people with diabetis)
 4. Docker container

**Graph Generarion**
 1. Python networkx
 2. Create graph in format for Cytoscope.js or sigma.js
 3. Save the most usual graphs (regimen,sexo,etc)

**Frontend**
 1. React - vite
 2. Cytoscope.js or sigma.js
 3. Recharts
 4. Docker container

**Cache**
 1. Cache graphs in mongodb

**Web server - entry point**
 1. nginx

**Containers**

*Always running*

                 nginx

                    │

        ┌───────────┴───────────┐

        │                       │

   React container         FastAPI container

                                    │

                            Mongo container


*One time*
python-polars-pymongo container
params input: year,municipio (cod divipola)
script : etl_ingest.py (review log)
output : ingest collection pacientes - logs(mongodb) : {anio:anio,municipio:municipio,num_records:##,date:stamptime}, logs logging

Example:

    docker compose run --rm etl \
        --year 2024 \
        --municipio 17001

**Architecture**

                           +----------------+
                           |     Nginx      |
                           +-------+--------+
                                   |
                  +----------------+----------------+
                  |                                 |
        +---------v---------+             +---------v---------+
        | React Frontend    |             | FastAPI Backend   |
        |                   |             |                   |
        +---------+---------+             +---------+---------+
                  |                                 |
                  | REST API                        |
                  +----------------+----------------+
                                   |
                           +-------v--------+
                           |    MongoDB     |
                           |----------------|
                           | patients       |
                           | graph_cache    |
                           | ingest_logs    |
                           +-------+--------+
                                   ^
                                   |
                    (writes once)  |
                           +-------+--------+
                           | ETL Container  |
                           | Polars/Python  |
                           | PyMongo        |
                           +----------------+
                                   |
                                   |
                          RIPS / BDUA files

--------------------------------------------------------------------
-- ETL ingest mongoDB
--------------------------------------------------------------------

**Docker dir**
    etl/
    
    ├── Dockerfile
    
    ├── requirements.txt
    
    ├── main.py
    
    ├── etl_ingest.py
    
    ├── config.py
    
    └── utils.py


**ETL**

etl_ingest.py export create_parquet(municipios,anio) , ingest_DB(municipio,logg)

    import argparse
    import logging
    from pathlib import Path
    
    from etl_ingest import (
        create_parquet,
        ingest,
        clean_parquet,
        load_municipios,
        already_ingest
    )
    
    
    def main():
    
        parser = argparse.ArgumentParser(
            description="RIPS ETL"
        )
    
        parser.add_argument(
            "--year",
            required=True,
            type=int,
            help="Processing year"
        )
    
        parser.add_argument(
            "--municipios-file",
            default="municipios.txt",
            help="File containing DIVIPOLA municipality codes"
        )
    
        args = parser.parse_args()
    
        logging.basicConfig(
            filename=f"/logs/ingest_{args.year}.log",
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s"
        )
    
        logger = logging.getLogger(__name__)
    
        logger.info("Starting ETL")
    
        municipios = load_municipios(
            Path(args.municipios_file)
        )
    
        create_parquet(
            year=args.year,
            municipios=municipios
        )
    
        for municipio in municipios:
    
            logger.info(f"Processing municipio {municipio}")

            if already_ingested(year, municipio):
                logger.info(
                    f"{municipio} already processed. Skipping."
                )
                continue
    
            try:
    
                ingest(
                    year=args.year,
                    municipio=municipio,
                    logger=logger
                )
    
                logger.info(
                    f"Municipio {municipio} completed"
                )
    
            except Exception:
    
                logger.exception(
                    f"Error processing municipio {municipio}"
                )
    
        clean_parquet(year=args.year)
    
        logger.info("ETL finished")
    
    
    if __name__ == "__main__":
        main()

