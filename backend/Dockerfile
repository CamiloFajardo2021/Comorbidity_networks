FROM python:3.12-slim

WORKDIR /app

# System deps needed to build some wheels (e.g. polars/pyarrow transitive deps).
# Kept minimal; remove if your platform ships prebuilt wheels only.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first so this layer is cached across code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the actual application code.
COPY . .

# Non-root user. CACHE_DIR (graph GML cache) is not a mounted volume, so it's
# created here and owned by the app user, not root.
RUN useradd --create-home --uid 1000 appuser \
    && mkdir -p /cache \
    && chown -R appuser:appuser /app /cache
USER appuser

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
