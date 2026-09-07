"""
FastAPI entrypoint for the Comorbidity Networks backend.

Route paths intentionally have NO /api prefix here: nginx strips /api/
before forwarding to this service (see nginx/nginx.conf), so the paths
below already match docs/05_backend_api.md as-is.
"""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient

from routers import analytics, graph

MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "rips_db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup: one connection pool, shared for the app's whole lifetime
    app.state.mongo_client = AsyncIOMotorClient(MONGO_URI)
    app.state.db = app.state.mongo_client[MONGO_DB_NAME]

    yield

    # shutdown: release it cleanly
    app.state.mongo_client.close()


app = FastAPI(
    title="Comorbidity Networks API",
    version="0.1.0",
    lifespan=lifespan,
)

# Dev convenience only: lets a frontend run outside Docker (e.g. `npm run
# dev` on the host) call this API directly. In production nginx serves
# frontend + backend on one origin, so this middleware never applies there.
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ORIGINS", "http://localhost:5173").split(","),
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health", tags=["meta"])
async def health():
    """Liveness/readiness check. Not part of docs/05_backend_api.md, but
    standard practice — and a natural fit for a future `healthcheck:` on
    the backend service in docker-compose.yaml, same pattern as mongodb's."""
    return {"status": "ok"}


app.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
app.include_router(graph.router, prefix="/graph", tags=["graph"])
app.include_router(patients.router, prefix="/patients", tags=["patients"])
