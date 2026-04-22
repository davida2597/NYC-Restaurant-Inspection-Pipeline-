# Use a stable Python version (match your runtime.txt mindset)
FROM python:3.11-slim

# Prevent Python from buffering logs (important for Docker logs)
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system dependencies (needed for psycopg, pandas, etc.)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first (for caching)
COPY requirements.txt .

# Install Python deps
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app
COPY . .

# Expose Streamlit port
EXPOSE 8501

# Optional: healthcheck for Streamlit itself
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Start Streamlit
CMD ["sh", "-c", "python wait_for_db.py && streamlit run streamlit_app.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true"]