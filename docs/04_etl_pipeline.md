# ETL Module

## Purpose

Load RIPS text files into MongoDB.

## Input

- RIPS_{YEAR}.txt
- BDUA_{YEAR-YEAR+1}.txt
- DIAGNOTICO_RELACIONADOS.txt

## Output

patients collection
log collection


## Container - JOB

The ETL will be run as a job in a docker run on demand. Below is an example of the initialize of the etl

    -- Docker file example

    services:
      mongodb:
        image: mongo:8
        ports:
          - "27017:27017"
        healthcheck:
          test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
          interval: 5s
          timeout: 5s
          retries: 10
    
      api:
        build: ./api
        depends_on:
          mongodb:
            condition: service_healthy
    
      etl:
        build: ./etl
        depends_on:
          mongodb:
            condition: service_healthy


The container will be connected to the RAW_DATA as volumne (to access) and will mount the information of the loggings in a determined directory.

    RAW_DATA -- DOCKER ETL -- MongoDB
    (Volume)       | mount
              logging files

## General steps

 0. Check in the logs files (collection in DB) if the year and municipio has arleady been update in mongoDB.
 1. Generate parquet file for RIPS for each municipio.
 2. Normalize,clean and join tables to produce the aggregate infomation for patient (for year).
 3. Clean parquet files.

## ETL steps 

Lazy init

  - BDUA_{year,year+1} filter by year.
  - RIPS_parquet_{municipio}
  - diag_rels

 1. Join DBUA with RIPS on PersonaID - rips_bdua
 2. Join rips_bdua with diag_rel on
    - PersonaID
    - fechaid
    - DxPrincipal
    - tipo_letter (tipo evento medico)
 3. Create structure for clinical information.
 4. Gruop by PersonaID
 5. Manage null values
 6. Crete _id (PersonaId + year)
 7. patients_df : collect
 8. Insert in batches into mongodb  
