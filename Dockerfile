# ---------------------------------------------------------------------------
# Dockerfile
# ---------------------------------------------------------------------------
# Single image used for BOTH the ETL container and the Streamlit container.
# docker-compose.yml overrides the CMD for each service:
#   etl       → python etl.py
#   streamlit → streamlit run streamlit_app.py ...
#
# Build:
#   docker build -t nyc-inspections .
#
# The dependencies layer (pip install) is cached separately from the source
# code layer (COPY . .) so that rebuilds after code-only changes are fast —
# only the last COPY triggers a re-build of subsequent layers.
# ---------------------------------------------------------------------------

# Use the slim variant to keep the image small (~120 MB vs ~900 MB for full)
FROM python:3.11-slim

# Set a working directory inside the container
WORKDIR /app

# Install system dependencies needed by psycopg3's C extension.
# --no-install-recommends keeps the layer small.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first so Docker can cache this layer.
# If requirements.txt doesn't change, this step is skipped on rebuild.
COPY requirements.txt .

# Install Python dependencies.
# --no-cache-dir saves ~50 MB by not writing the pip download cache.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the source code.
# This layer changes most often so it goes last to maximise cache hits.
COPY . .

# Default command — overridden by docker-compose.yml per service.
# Running etl.py by default makes sense for a standalone container test.
CMD ["python", "etl.py"]
