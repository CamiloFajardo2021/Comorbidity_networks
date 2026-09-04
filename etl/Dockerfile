FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Non-root user. Override APP_UID/APP_GID at build time to match the host
# user that owns ./logs, ./parquet and the external /mnt/rips_data drive,
# the same UID/GID mechanics used for the mongodb bind mount - otherwise
# this container can end up unable to write to those mounted volumes.
#   docker compose build --build-arg APP_UID=$(id -u) --build-arg APP_GID=$(id -g) etl
ARG APP_UID=1000
ARG APP_GID=1000
RUN groupadd -g "${APP_GID}" appuser 2>/dev/null || true \
    && useradd --create-home -u "${APP_UID}" -g "${APP_GID}" appuser 2>/dev/null || true \
    && chown -R appuser:appuser /app
USER appuser

# This is a batch job (restart: "no" in docker-compose), not a long-running
# server, so there's no EXPOSE / HEALTHCHECK here.
# --anio and --municipios are required CLI args, passed at `docker compose run`
# (not baked into the image), e.g.:
#   docker compose run etl --anio 2024 --municipios 17001 25307
ENTRYPOINT ["python", "pipeline2.py"]
