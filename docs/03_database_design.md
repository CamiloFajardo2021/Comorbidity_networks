# Database Design

## 1. Purpose

The `rips_db` database is designed to store **patient-centered healthcare records** derived from RIPS datasets.

Its primary objective is to enable efficient filtering of patients according to sociodemographic characteristics (e.g., sex, age, ethnicity, disability status, and health insurance information) and to support the construction and analysis of **comorbidity networks** based on their medical diagnoses.

The database is optimized for analytical workloads rather than transactional operations.

---

# 2. Data Sources

The database is populated through an ETL pipeline that integrates information from multiple sources.

| Source                                 | Description                                                                                |
| -------------------------------------- | ------------------------------------------------------------------------------------------ |
| `BDUA_{year}.txt`                      | Sociodemographic information of the population                                             |
| `RIPS_{year}.txt`                      | Medical events including consultations, procedures, emergency visits, and hospitalizations |
| `Diagnosticos_relacionados_{year}.txt` | Related (secondary) diagnoses associated with each medical event                           |

The ETL process merges these sources into a single patient-centered document.

---

# 3. Database Organization

The database consists of two primary collections:

* `patients`
* `etl_logs`

## 3.1 Patients Collection

Each document represents **one patient for one calendar year**.

This design enables longitudinal analyses while preserving yearly snapshots of patient healthcare utilization.

Example document:

```python
{
    "_id": "105204889_2014",

    "PersonaID": 105204889,

    "sexo": "F",

    "edad": 0,

    "municipio": 17001,

    "departamento": 17,

    "zona": "U",

    "anio": "2014",

    "etnia": "NO REPORTADO",

    "actividad_economica": "Default",

    "discapacidad": [],

    "consultas": [

        {
            "fecha": datetime(...),

            "tipo_evento": "CONSULTA",

            "diag_prin": "L239",

            "diag_rel": [],

            "prestador": 170010003401,

            "tipo_atencion": "CONSULTA",

            "costo_consulta": 35200,

            "costo_procedimiento": 0,

            "codigo_procedimiento": 890201,

            "dias_estancia": 0
        }

    ]
}
```

Each consultation contains the principal diagnosis and its associated related diagnoses, which are later used to generate weighted comorbidity networks.

---

## 3.2 ETL Logs Collection

The `etl_logs` collection stores metadata about each ingestion process.

Example:

```python
{
    "year": 2014,

    "started_at": "...",

    "finished_at": "...",

    "status": "completed",

    "records_processed": 48721342,

    "version": 1
}
```

This collection prevents duplicate imports and provides traceability for data processing operations.

---

# 4. Index Strategy

Since the primary workload consists of filtering patient cohorts before graph generation, indexes are created on frequently queried fields.

Recommended indexes:

* `_id`
* `PersonaID`
* `anio`
* `sexo`
* `edad`
* `municipio`
* `departamento`
* `zona`
* `etnia`
* `discapacidad`

Compound indexes may be added for common query patterns, for example:

```javascript
{ anio: 1, sexo: 1 }

{ anio: 1, municipio: 1 }

{ anio: 1, edad: 1 }

{ anio: 1, etnia: 1 }
```

---

# 5. Query Patterns

The database is intended to support cohort selection prior to graph construction.

Example: retrieve female patients.

```python
collection.find(
    {
        "sexo": "F"
    }
)
```

Example: retrieve patients between 40 and 60 years old.

```python
collection.find(
    {
        "edad": {
            "$gte": 40,
            "$lte": 60
        }
    }
)
```

Example: retrieve urban female patients from 2014.

```python
collection.find(
    {
        "anio": "2014",

        "sexo": "F",

        "zona": "U"
    }
)
```

---

# 6. Design Rationale

The database follows a **patient-centered document model**, where all medical events associated with a patient during a given year are stored within a single document.

This approach offers several advantages:

* Reduces the number of database joins
* Improves filtering performance
* Simplifies patient trajectory reconstruction
* Enables efficient generation of diagnosis combinations
* Minimizes disk reads during graph construction

The document model is particularly well suited for analytical pipelines in which complete patient histories are processed together to derive comorbidity relationships.

---

# 7. Future Extensions

Future versions of the database may incorporate:

* Medication records
* Laboratory results
* Mortality information
* Temporal disease trajectories
* Precomputed comorbidity edges
* Community assignments
* Centrality metrics
* Risk scores derived from graph analytics

```
```

    
