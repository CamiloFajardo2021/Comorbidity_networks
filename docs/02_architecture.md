               TXT Files

                    │

                 ETL Engine

                    │

               MongoDB Database

                    │

            Graph Construction

                    │

               FastAPI Backend

                    │

              React Frontend


Directory management

    comorbidity-app/
    
    │
    ├── docker-compose.yml
    │
    ├── backend/
    │     ├── Dockerfile
    │     ├── requirements.txt
    │     └── app/
    │
    ├── frontend/
    │     ├── Dockerfile
    │     ├── package.json
    │     └── src/
    │
    ├── mongodb/
    │     └── init.js
    │
    ├── data/
    │     ├── raw/
    │     └── processed/
    │
    └── docs/



                  docker-compose

        ┌─────────────────────────┐
        │        MongoDB          │
        │   (always running)      │
        └───────────┬─────────────┘
                    │
          mongodb://mongodb:27017
                    │
        ┌───────────▼─────────────┐
        │        FastAPI          │
        │    (always running)     │
        └───────────┬─────────────┘
                    │
                    │ REST API
                    │
        ┌───────────▼─────────────┐
        │        React            │
        │    (always running)     │
        └─────────────────────────┘


        ETL (run only when needed)

        ┌─────────────────────────┐
        │    Python ETL image     │
        │  python ingest.py 2014  │
        └───────────┬─────────────┘
                    │
                    ▼
                 MongoDB
