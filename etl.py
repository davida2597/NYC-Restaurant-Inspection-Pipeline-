"""
etl.py
------
DOCKER entry-point script: runs the full ETL pipeline against a local
PostgreSQL container.  This is what docker-compose calls inside the
'etl' service — no Supabase account needed.

Pipeline steps (all in one process):
    1. EXTRACT  — fetch inspection records from the NYC Open Data API
    2. TRANSFORM — clean the raw data using the existing cleaning/ module
    3. LOAD     — create the schema and insert into local PostgreSQL

How much data is fetched?
    Controlled by ETL_FETCH_DAYS in .env (default: 90 days).
    Keeping this low makes docker-compose up --build finish quickly.
    Increase it if you want a longer history for the dashboard.

Usage (normally called by docker-compose, but also runnable directly)
-----
    python etl.py

Requirements in .env
--------------------
    API_BASE_URL, API_APP_TOKEN
    DATABASE_URL  (points to local postgres container, e.g.
                   postgresql://postgres:postgres@db:5432/nyc_inspections)
    ETL_FETCH_DAYS  (optional, default 90)
    API_BATCH_SIZE  (optional, default 1000)
    API_REQUEST_DELAY (optional, default 0.5)
"""

import os
import sys
import time
import traceback
from datetime import date, timedelta

import pandas as pd
import psycopg
import requests
from dotenv import load_dotenv

# Import the shared loading utilities
from db_loader import create_schema, load_dataframe, SCHEMA_SQL

# Import every cleaning step from the existing cleaning module.
# The order here must match the pipeline order in clean_data.py.
from cleaning import (
    enforce_column_layout,
    normalize_nulls,
    strip_whitespace,
    normalize_whitespace,
    normalize_caps,
    normalize_boro,
    normalize_coords,
    parse_dates,
    infer_dates,
    infer_grades,
    clean_phone,
    validate_types,
    drop_nulls,
    remove_duplicates,
)

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_BASE_URL      = os.environ["API_BASE_URL"]
API_APP_TOKEN     = os.environ["API_APP_TOKEN"]
API_BATCH_SIZE    = int(os.environ.get("API_BATCH_SIZE", 1000))
API_REQUEST_DELAY = float(os.environ.get("API_REQUEST_DELAY", 0.5))
DATABASE_URL      = os.environ["DATABASE_URL"]

# How many days of inspection history to fetch.
# 90 days is a practical default for Docker (fast startup, still useful data).
ETL_FETCH_DAYS = int(os.environ.get("ETL_FETCH_DAYS", 90))


# ---------------------------------------------------------------------------
# Cleaning pipeline
# Order matters — must match clean_data.py CLEANING_PIPELINE list
# ---------------------------------------------------------------------------
CLEANING_PIPELINE = [
    enforce_column_layout,
    normalize_nulls,
    strip_whitespace,
    normalize_whitespace,
    normalize_caps,
    normalize_boro,
    normalize_coords,
    parse_dates,
    infer_dates,
    infer_grades,
    clean_phone,
    validate_types,
    drop_nulls,
    remove_duplicates,
]


# ---------------------------------------------------------------------------
# Step 1 — EXTRACT: fetch from NYC Open Data API
# ---------------------------------------------------------------------------

def fetch_inspections(start: date, end: date) -> pd.DataFrame:
    """
    Page through the NYC Open Data API and return all inspection records
    where inspection_date falls within [start, end].

    The API uses SoQL $where clauses and returns JSON.
    We page in batches of API_BATCH_SIZE (default 1000) until we get
    fewer rows than we asked for (indicating the last page).
    """
    all_rows = []
    offset   = 0
    page     = 1

    # SoQL date filter — inclusive on both ends
    where_clause = (
        f"inspection_date >= '{start}T00:00:00' AND "
        f"inspection_date <= '{end}T23:59:59'"
    )

    print(f"[extract] Fetching records: {start} → {end}")
    print(f"[extract] Filter: {where_clause}")

    while True:
        params = {
            "$limit":      API_BATCH_SIZE,
            "$offset":     offset,
            "$order":      "inspection_date ASC, camis ASC",
            "$where":      where_clause,
            "$$app_token": API_APP_TOKEN,
        }

        try:
            resp = requests.get(API_BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"  [error] API request failed on page {page}: {e}")
            raise

        rows = resp.json()

        if not rows:
            # Empty response means we've exhausted all records
            break

        all_rows.extend(rows)
        print(
            f"  page {page:>3}  fetched {len(rows):>5} rows  "
            f"(total so far: {len(all_rows):>7})"
        )

        # Fewer rows than we asked for → this was the last page
        if len(rows) < API_BATCH_SIZE:
            break

        offset += API_BATCH_SIZE
        page   += 1
        time.sleep(API_REQUEST_DELAY)  # be polite to the API

    df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
    print(f"[extract] Done — {len(df):,} total rows fetched\n")
    return df


# ---------------------------------------------------------------------------
# Step 2 — TRANSFORM: run the cleaning pipeline
# ---------------------------------------------------------------------------

def _force_object_dtype(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast every column to plain object dtype.

    pandas 2.x introduced StringDtype (dtype.name == 'string'), which is
    incompatible with pd.to_numeric() used inside normalize_coords and
    validate_types.  Calling astype(object) on the whole frame is the safest
    way to guarantee every column is numpy object before a cleaning step runs.
    Date columns are already converted by parse_dates so they stay as datetime;
    astype(object) on a datetime64 column is a no-op for our purposes because
    validate_types checks those separately.
    """
    for col in df.columns:
        try:
            # Only touch extension-array-backed columns (StringDtype, BooleanDtype …)
            # Leave numpy dtypes (object, int64, float64, datetime64) alone.
            if hasattr(df[col].dtype, '_is_numeric'):
                continue
            if str(df[col].dtype) not in ('object', 'int64', 'float64', 'datetime64[ns]'):
                df[col] = df[col].astype(object)
        except Exception:
            pass
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply every cleaning function in CLEANING_PIPELINE sequentially.
    Each function receives a DataFrame and returns a (possibly modified)
    DataFrame — same pattern as clean_data.py.
    """
    print("[transform] Running cleaning pipeline …")
    before = len(df)

    for func in CLEANING_PIPELINE:
        # Force all extension-array columns (e.g. StringDtype from pandas 2.x)
        # back to plain object BEFORE each step so pd.to_numeric() never sees
        # a StringDtype column.
        df = _force_object_dtype(df)

        try:
            df = func(df)
        except Exception as e:
            # Print which function failed and the dtypes of every non-object
            # column so we can diagnose future issues quickly.
            print(f"\n[error] {func.__name__} raised: {e}")
            print("[debug] Non-object column dtypes at time of failure:")
            for col in df.columns:
                if str(df[col].dtype) != 'object':
                    print(f"  {col}: {df[col].dtype}")
            traceback.print_exc()
            raise

    after = len(df)
    print(f"[transform] Done — {before:,} rows in, {after:,} rows out\n")
    return df


# ---------------------------------------------------------------------------
# Step 3 — LOAD: write to PostgreSQL
# ---------------------------------------------------------------------------

def load(df: pd.DataFrame, conn):
    """
    Create the schema (if it doesn't exist) and load the cleaned DataFrame
    into the 4 normalised tables using the shared db_loader module.
    """
    print("[load] Creating / verifying database schema …")
    create_schema(conn)

    print("[load] Inserting data …")
    counts = load_dataframe(df, conn)
    return counts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    today = date.today()
    start = today - timedelta(days=ETL_FETCH_DAYS)

    print(f"\n{'='*60}")
    print("  etl.py — NYC Restaurant Inspection ETL (Docker)")
    print(f"  Fetch window  : {start} → {today}  ({ETL_FETCH_DAYS} days)")
    print(f"  API batch size: {API_BATCH_SIZE} rows per request")
    print(f"  Database      : {DATABASE_URL.split('@')[-1]}")  # hide credentials
    print(f"{'='*60}\n")

    # ------------------------------------------------------------------
    # 1. EXTRACT
    # ------------------------------------------------------------------
    try:
        raw_df = fetch_inspections(start=start, end=today)
    except Exception as e:
        print(f"\n[error] Extraction failed: {e}")
        sys.exit(1)

    if raw_df.empty:
        print("[done] API returned 0 rows. Nothing to load.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # 2. TRANSFORM
    # ------------------------------------------------------------------
    try:
        clean_df = clean(raw_df)
    except Exception as e:
        print(f"\n[error] Cleaning failed: {e}")
        traceback.print_exc()
        sys.exit(1)

    if clean_df.empty:
        print("[done] All rows were dropped during cleaning. Nothing to load.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # 3. LOAD
    # ------------------------------------------------------------------
    print("[db] Connecting to PostgreSQL …")
    try:
        conn = psycopg.connect(DATABASE_URL)
    except Exception as e:
        print(f"[error] Could not connect to database: {e}")
        sys.exit(1)

    print("[db] Connected.\n")

    try:
        counts = load(clean_df, conn)
    except Exception as e:
        print(f"\n[error] Load failed: {e}")
        conn.close()
        sys.exit(1)

    conn.close()

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("  ETL COMPLETE")
    print(f"  Restaurants  : {counts['restaurants']:>8,}")
    print(f"  Violations   : {counts['violations']:>8,}")
    print(f"  Inspections  : {counts['inspections']:>8,}")
    print(f"  Insp. links  : {counts['iv']:>8,}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
