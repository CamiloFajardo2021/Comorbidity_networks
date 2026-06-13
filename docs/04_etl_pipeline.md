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

## Steps

1. Read files
2. Validate schema
3. Clean null values
4. Merge BDUA
5. Aggregate by patient
6. Insert into MongoDB
