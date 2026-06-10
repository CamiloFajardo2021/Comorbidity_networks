# Comorbidity Graph Platform

> An open-source platform for building, analyzing, and visualizing comorbidity networks from Colombian RIPS (Registro Individual de Prestación de Servicios de Salud) data.

---

## Overview

The **Comorbidity Graph Platform** is designed to transform raw healthcare records into patient-centered datasets and weighted disease networks, enabling epidemiological studies, network analysis, and interactive visualization.

The platform provides an end-to-end pipeline:

* 📂 Ingestion of RIPS text files
* 🧹 Data validation and cleaning
* 👤 Patient reconstruction
* 🗄️ Storage in MongoDB
* 🕸️ Comorbidity graph generation
* 📊 Network analysis
* 🌐 Interactive web visualization

---

## Project Architecture

```text
                RIPS TXT Files
                       │
                       ▼
              ETL Pipeline (Python)
                       │
                       ▼
                  MongoDB Database
                       │
                       ▼
            Comorbidity Graph Builder
                       │
                       ▼
                 FastAPI REST API
                       │
                       ▼
                React Web Application
```

---

## Technology Stack

| Component      | Technology              |
| -------------- | ----------------------- |
| Backend        | Python + FastAPI        |
| ETL            | Python + Polars         |
| Database       | MongoDB                 |
| Frontend       | React + Vite            |
| Graph Analysis | NetworkX / igraph       |
| Containers     | Docker & Docker Compose |

---

## Repository Structure

```text
comorbidity-platform/

├── backend/
│   ├── app/
│   ├── etl/
│   ├── graph/
│   └── api/
│
├── frontend/
│
├── mongodb/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── docs/
│
├── docker-compose.yml
│
└── README.md
```

---

## Features

* Import RIPS data from TXT files
* Incremental ingestion by year
* Patient-centered document model
* Automatic generation of weighted comorbidity graphs
* Network metrics (degree, betweenness, PageRank, etc.)
* Community detection algorithms
* Interactive graph exploration
* Filtering by demographic and temporal variables
* Export to GraphML, GEXF, and CSV

---

## Development Roadmap

### Phase 1

* [ ] Docker infrastructure
* [ ] MongoDB deployment
* [ ] Project structure

### Phase 2

* [ ] ETL pipeline
* [ ] Data validation
* [ ] Incremental ingestion

### Phase 3

* [ ] Patient reconstruction
* [ ] MongoDB optimization
* [ ] Index creation

### Phase 4

* [ ] Comorbidity graph generation
* [ ] Weighted edge computation
* [ ] Graph storage

### Phase 5

* [ ] FastAPI backend
* [ ] REST API
* [ ] Authentication

### Phase 6

* [ ] React frontend
* [ ] Dashboard
* [ ] Graph explorer

### Phase 7

* [ ] Community detection
* [ ] Centrality analysis
* [ ] Statistical reports

---

## Getting Started

Clone the repository:

```bash
git clone https://github.com/your-username/comorbidity-graph-platform.git

cd comorbidity-graph-platform
```

Start the infrastructure:

```bash
docker compose up -d
```

Run the ETL pipeline:

```bash
docker compose run etl ingest 2024
```

Start the backend:

```bash
docker compose up backend
```

Start the frontend:

```bash
docker compose up frontend
```

---

## Long-Term Vision

The goal of this project is to provide a scalable platform for studying disease co-occurrence patterns through graph theory and network science.

By representing diagnoses as interconnected networks, the platform aims to support:

* Public health research
* Epidemiological surveillance
* Healthcare planning
* Disease trajectory analysis
* Resource allocation studies
* Clinical decision support research

---

## License

This project is released under the MIT License.

---

## Author

Developed as an open-source project for the analysis and visualization of healthcare networks using RIPS data and graph-based methodologies.

## Data Source

> **Note**
>
> This project is designed to process healthcare data provided by the **Ministerio de Salud y Protección Social de Colombia** through the **Registro Individual de Prestación de Servicios de Salud (RIPS)**.
>
> The datasets used in this project are **anonymized** and do **not** contain directly identifiable personal information. The platform is intended exclusively for research, epidemiological analysis, network science, and public health applications, respecting applicable data protection regulations and ethical principles.
>
> This repository **does not distribute or include the original RIPS datasets**. Users are responsible for obtaining access to the data through the appropriate legal and institutional channels and for ensuring compliance with the corresponding data use agreements.

## Disclaimer

This software is an independent open-source project and is **not affiliated with or endorsed by the Ministerio de Salud y Protección Social de Colombia**.

The repository contains source code and documentation only. No confidential or personally identifiable health information is included. Any analysis performed with this platform should be conducted using properly authorized and anonymized datasets, in accordance with applicable laws, institutional policies, and ethical guidelines.

