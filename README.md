# NYC Restaurant Inspection Analysis Dashboard

## Project Overview

This project analyzes restaurant inspection data from the **New York City Department of Health and Mental Hygiene (DOHMH)** to identify patterns and potential correlations between restaurant inspection outcomes and environmental factors such as restaurant density, location, and public reviews.

The core goal of this project is to provide an **interactive dashboard** that allows users to explore the inspection dataset visually and uncover trends using filters and data exploration tools.

Dataset source: https://data.cityofnewyork.us/d/43nn-pn8j

---

## Objectives

- Are restaurant inspections stricter in certain boroughs or neighborhoods?
- Is there a relationship between restaurant **inspection scores** and **customer reviews**?
- Do areas with a **high density of restaurants** experience different inspection outcomes?
- Are there geographic clusters of restaurants with consistently higher or lower inspection scores?

---

## Technologies Used

| Tool | Purpose |
|---|---|
| Python 3.11 | Core language |
| Pandas | Data cleaning and manipulation |
| Requests | NYC Open Data API calls |
| psycopg3 | PostgreSQL driver |
| SQLAlchemy | ORM / connection layer for Streamlit |
| Supabase | Cloud storage buckets + hosted PostgreSQL (production) |
| Streamlit | Interactive dashboard |
| Plotly | Charts and maps |
| Docker + Docker Compose | Containerization and orchestration |
| GitHub Actions | Automated daily pipeline updates |

---

## Repository Structure

```
NYC-Restaurant-Inspection-Pipeline/
│
├── cleaning/                    # Modular cleaning functions (one per file)
│   ├── __init__.py
│   ├── enforce_column_layout.py
│   ├── normalize_nulls.py
│   ├── strip_whitespace.py
│   ├── normalize_whitespace.py
│   ├── normalize_caps.py
│   ├── normalize_boro.py
│   ├── normalize_coords.py
│   ├── parse_dates.py
│   ├── infer_dates.py
│   ├── infer_grades.py
│   ├── clean_phone.py
│   ├── validate_types.py
│   ├── drop_nulls.py
│   └── remove_duplicates.py
│
├── fetch_all.py                 # One-time historical backfill (Supabase pipeline)
├── fetch_new.py                 # Incremental daily fetch (Supabase pipeline)
├── clean_data.py                # Parallel cleaning of raw CSVs in Supabase Storage
├── port_data.py                 # Load cleaned CSVs from Supabase Storage → PostgreSQL
├── setup_db.py                  # One-time DB schema + Supabase bucket creation
│
├── db_loader.py                 # Shared DB schema SQL + load_dataframe() utility
├── etl.py                       # Docker entry point: API → clean → local PostgreSQL
├── streamlit_app.py             # Interactive Streamlit dashboard
│
├── Dockerfile                   # Container image (used for both ETL and Streamlit)
├── docker-compose.yml           # Orchestrates db + etl + streamlit services
├── requirements.txt             # Python dependencies
├── env.example                  # Template for .env (no secrets)
└── README.md
```

---

## Data Processing Pipeline

### Production Pipeline (Supabase + GitHub Actions)

```
NYC Open Data API
       │
       ▼
fetch_all.py          ← One-time full backfill (2010 → today)
fetch_new.py          ← Daily incremental update (via GitHub Actions cron)
       │
       ▼
Supabase Storage [raw_data_csv]
       │
       ▼
clean_data.py         ← Parallel cleaning (3 workers)
       │
       ▼
Supabase Storage [cleaned_data_csv]
       │
       ▼
port_data.py          ← Normalize and load into PostgreSQL
       │
       ▼
Supabase PostgreSQL   ← 4-table normalized schema
       │
       ▼
streamlit_app.py      ← Interactive dashboard
```

### Docker Pipeline (local, self-contained)

```
NYC Open Data API
       │
       ▼
etl.py  (fetch → clean → load, all in one process)
       │
       ▼
Local PostgreSQL container
       │
       ▼
streamlit_app.py  (http://localhost:8501)
```

---

## Database Schema

The raw data has one row per violation per inspection. We normalize this into four tables to eliminate redundancy:

```
restaurants            — one row per restaurant (keyed by camis)
violations             — one row per violation code (lookup table)
inspections            — one row per inspection visit (camis + date + type)
inspection_violations  — junction: which violations appeared in which inspection
```

---

## Dashboard Features

The Streamlit dashboard at `streamlit_app.py` includes:

1. **Summary Metrics** — total restaurants, inspections, grade A rate, date range
2. **Grade Distribution** — pie chart of A / B / C / other grades
3. **Score by Borough** — horizontal bar chart (lower score = better)
4. **Score Trend Over Time** — monthly average inspection score line chart
5. **Top 10 Violations** — most frequently cited violation codes, coloured by severity
6. **Score by Cuisine Type** — average score for the top 25 cuisine categories
7. **Restaurant Map** — scatter map coloured by most recent inspection grade

Sidebar filters: borough, date range, grade (map only).

---

## How to Run with Docker (recommended)

This is the simplest way to run the full pipeline with a single command.

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- A free NYC Open Data app token from https://data.cityofnewyork.us/profile/app_tokens

### Step 1 — Set up your `.env` file

```bash
cp env.example .env
```

Open `.env` and fill in at minimum:

```
API_BASE_URL=https://data.cityofnewyork.us/resource/43nn-pn8j.json
API_APP_TOKEN=your_token_here

ETL_FETCH_DAYS=90    # how many days of history to load (90 = ~2–5 min)
```

You can leave all Supabase variables blank for Docker — they are only needed for the production Supabase pipeline.

### Step 2 — Build and run

```bash
docker-compose up --build
```

This will:
1. Start a **PostgreSQL 15** container with an empty database
2. Run **etl.py** — fetches the last `ETL_FETCH_DAYS` days from the NYC API, cleans the data, and loads it into PostgreSQL
3. Start **Streamlit** once the ETL finishes

### Step 3 — Open the dashboard

Navigate to **http://localhost:8501** in your browser.

### Stopping

```bash
docker-compose down        # stop containers, keep database data
docker-compose down -v     # stop containers AND delete database (clean slate)
```

---

## How to Run Locally (without Docker)

### Prerequisites

```bash
pip install -r requirements.txt
cp env.example .env
# fill in .env with your values
```

### Run the Docker-style ETL against a local Postgres

```bash
# Set DATABASE_URL in .env to your local Postgres, then:
python etl.py
```

### Run the Streamlit dashboard

```bash
streamlit run streamlit_app.py
```

### Production Supabase pipeline (team use)

```bash
# One-time setup (run once per project)
python setup_db.py

# Full historical backfill (run once)
python fetch_all.py

# Daily incremental update (normally triggered by GitHub Actions)
python fetch_new.py

# Clean raw CSVs in Supabase Storage
python clean_data.py

# Load cleaned CSVs into PostgreSQL
python port_data.py
```

---

## Environment Variables

See `env.example` for full documentation. Key variables:

| Variable | Used by | Description |
|---|---|---|
| `API_BASE_URL` | fetch scripts, etl.py | NYC Open Data endpoint |
| `API_APP_TOKEN` | fetch scripts, etl.py | Your NYC Open Data token |
| `SUPABASE_URL` | Supabase scripts | Supabase project URL |
| `SUPABASE_KEY` | Supabase scripts | Publishable key |
| `SUPABASE_SECRET_KEY` | port_data.py, setup_db.py | Secret key (DB writes) |
| `DATABASE_URL` | all DB scripts | PostgreSQL connection string |
| `ETL_FETCH_DAYS` | etl.py | Days of history to fetch in Docker (default 90) |

---

## Contributors

| Name | Contributions |
|---|---|
| **David Arnold** | |
| **Erica Chen** | |
| **Zarko Dimitrov** | |
| **Arnav Karnati** | Data pipeline architecture, fetch scripts, cleaning module, database schema, Docker setup, Streamlit dashboard |
