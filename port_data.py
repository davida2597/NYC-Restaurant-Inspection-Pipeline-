"""
port_data.py
------------
Loads cleaned CSV files from Supabase Storage into the normalized
PostgreSQL database. Designed for both full-load (FETCH_MODE="all")
and incremental runs (FETCH_MODE="incremental").

Performance approach:
    Uses executemany() with psycopg's fast binary protocol for bulk
    inserts rather than one row at a time. For the largest table
    (inspection_violations) we use the COPY protocol via
    psycopg.copy() which is the fastest possible method for bulk
    inserts into PostgreSQL — typically 10-50x faster than executemany.

Usage:
    python port_data.py

    Set FETCH_MODE="all" to process every file in the bucket.
    Set FETCH_MODE="incremental" to skip rows already in the DB
    based on MAX(record_date).
"""

import io
import os
from datetime import date

import pandas as pd
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# =============================================================================
# CONFIG
# =============================================================================
FETCH_MODE   = os.environ.get("FETCH_MODE", "incremental")  # "all" or "incremental"
BUCKET_NAME  = os.environ.get("CLEANED_BUCKET", "cleaned_data_csv")

SUPABASE_URL        = os.environ["SUPABASE_URL"]
SUPABASE_SECRET_KEY = os.environ["SUPABASE_SECRET_KEY"]
DATABASE_URL        = os.environ["DATABASE_URL"]

# Explicit dtypes prevent pandas from silently mangling types on load
DTYPES = {
    "camis":               "string",
    "dba":                 "string",
    "boro":                "string",
    "building":            "string",
    "street":              "string",
    "zipcode":             "string",
    "phone":               "string",
    "cuisine_description": "string",
    "inspection_type":     "string",
    "action":              "string",
    "violation_code":      "string",
    "violation_description": "string",
    "critical_flag":       "string",
    "score":               "Int16",
    "grade":               "string",
    "latitude":            "Float64",
    "longitude":           "Float64",
    "community_board":     "Int16",
    "council_district":    "Int16",
    "census_tract":        "Float64",
    "bin":                 "Int64",
    "bbl":                 "Int64",
    "nta":                 "string",
    "location":            "string",
}

supabase = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

# =============================================================================
# UTILITIES
# =============================================================================

def native(value):
    """
    Convert a pandas scalar to a plain Python type safe for psycopg.
    pandas NA, NaN, NaT, and None all become Python None.
    pandas Int16/Int64/Float64 extension types become int/float.
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    # Unwrap pandas extension types (Int16, Float64, etc.) to plain Python
    if hasattr(value, "item"):
        return value.item()
    return value


def rows_as_tuples(df: pd.DataFrame, columns: list) -> list[tuple]:
    """Extract a list of tuples from a DataFrame, converting every value via native()."""
    return [
        tuple(native(row[col]) for col in columns)
        for _, row in df[columns].iterrows()
    ]

# =============================================================================
# CURSOR DATE
# =============================================================================

def get_cursor_date() -> date:
    """Return the latest record_date already in the database."""
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(record_date) FROM inspections;")
                result = cur.fetchone()[0]
                if result:
                    return result if isinstance(result, date) else result.date()
    except Exception as e:
        print(f"[cursor] Error querying database: {e}")

    fallback = date(2000, 1, 1)
    print(f"[cursor] Database empty or unreachable — using fallback {fallback}")
    return fallback

# =============================================================================
# FILE LIST
# =============================================================================

def get_all_files() -> list[str]:
    files = supabase.storage.from_(BUCKET_NAME).list()
    return [f["name"] for f in files if f["name"].endswith(".csv")]

# =============================================================================
# BULK INSERT HELPERS
# =============================================================================

def bulk_insert_restaurants(cur, df: pd.DataFrame):
    """
    Upsert the unique restaurants found in this CSV.
    ON CONFLICT (camis) DO NOTHING — existing restaurants are left unchanged.
    All rows for a restaurant share the same camis, so we deduplicate first.
    """
    cols = [
        "camis", "dba", "boro", "building", "street", "zipcode", "phone",
        "cuisine_description", "latitude", "longitude",
        "community_board", "council_district", "census_tract",
        "bin", "bbl", "nta",
    ]
    restaurants = df.drop_duplicates(subset=["camis"])[cols]
    data = rows_as_tuples(restaurants, cols)

    if not data:
        return 0

    cur.executemany("""
        INSERT INTO restaurants (
            camis, dba, boro, building, street, zipcode, phone,
            cuisine_description, latitude, longitude,
            community_board, council_district, census_tract,
            bin, bbl, nta, created_at, updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, NOW(), NOW()
        )
        ON CONFLICT (camis) DO NOTHING
    """, data)

    return len(data)


def bulk_insert_violations(cur, df: pd.DataFrame):
    """
    Insert unique violation codes.
    Rows with a null violation_code are skipped (uninspected restaurants).
    ON CONFLICT (violation_code) DO NOTHING.
    """
    cols = ["violation_code", "violation_description", "critical_flag"]
    violations = (
        df.dropna(subset=["violation_code"])
          .drop_duplicates(subset=["violation_code"])[cols]
    )
    data = rows_as_tuples(violations, cols)

    if not data:
        return 0

    cur.executemany("""
        INSERT INTO violations (violation_code, violation_description, critical_flag, created_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (violation_code) DO NOTHING
    """, data)

    return len(data)


def bulk_insert_inspections(cur, df: pd.DataFrame) -> dict:
    """
    Insert unique inspection visits (camis + inspection_date + inspection_type).
    Returns a dict mapping (camis, inspection_date, inspection_type) -> inspection id
    for every inspection in this file, whether newly inserted or pre-existing.

    Strategy:
        1. Bulk insert all unique inspections with ON CONFLICT DO NOTHING.
        2. Bulk SELECT the ids for ALL unique (camis, date, type) combinations
           in this file — this catches both newly inserted rows and pre-existing ones.

    This avoids a per-row SELECT after every conflict, which was the main
    cause of slowness in the original script.
    """
    cols = [
        "camis", "inspection_date", "inspection_type", "action",
        "score", "grade", "grade_date", "record_date",
    ]
    inspections = df.drop_duplicates(subset=["camis", "inspection_date", "inspection_type"])[cols]
    data = rows_as_tuples(inspections, cols)

    if not data:
        return {}

    # Step 1 — bulk insert, skip conflicts silently
    cur.executemany("""
        INSERT INTO inspections (
            camis, inspection_date, inspection_type, action,
            score, grade, grade_date, record_date, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (camis, inspection_date, inspection_type) DO NOTHING
    """, data)

    # Step 2 — one bulk SELECT to retrieve ids for every inspection in this file.
    # Covers both newly inserted rows and pre-existing ones that were skipped.
    #
    # We use UNNEST with explicit type casts instead of ANY(%s) with a list of
    # tuples. PostgreSQL cannot infer the type of an "anonymous composite" passed
    # via ANY(), raising "input of anonymous composite types is not implemented".
    # UNNEST with typed arrays sidesteps this entirely.
    camis_list = [native(r["camis"])           for _, r in inspections.iterrows()]
    date_list  = [native(r["inspection_date"])  for _, r in inspections.iterrows()]
    type_list  = [native(r["inspection_type"])  for _, r in inspections.iterrows()]

    cur.execute("""
        SELECT i.id, i.camis, i.inspection_date, i.inspection_type
        FROM inspections i
        JOIN UNNEST(
            %s::text[],
            %s::date[],
            %s::text[]
        ) AS lookup(camis, inspection_date, inspection_type)
          ON  i.camis           = lookup.camis
          AND i.inspection_date = lookup.inspection_date
          AND i.inspection_type = lookup.inspection_type
    """, (camis_list, date_list, type_list))

    inspection_ids = {}
    for row in cur.fetchall():
        id_, camis, insp_date, insp_type = row
        inspection_ids[(camis, insp_date, insp_type)] = id_

    return inspection_ids


def bulk_insert_inspection_violations(cur, df: pd.DataFrame, inspection_ids: dict):
    """
    Insert the junction table rows linking inspections to violations.
    Uses COPY (the fastest PostgreSQL bulk insert method) rather than
    executemany, since this table can have the most rows.

    Rows with null violation_code are skipped.
    ON CONFLICT (inspection_id, violation_code) DO NOTHING via a temp table + merge.

    COPY does not natively support ON CONFLICT, so we:
        1. COPY into a temp table
        2. INSERT from temp into the real table with ON CONFLICT DO NOTHING
    """
    rows_df = df.dropna(subset=["violation_code"])

    if rows_df.empty:
        return 0

    # Build list of (inspection_id, violation_code) pairs
    data = []
    for _, row in rows_df.iterrows():
        key = (native(row["camis"]), native(row["inspection_date"]), native(row["inspection_type"]))
        inspection_id = inspection_ids.get(key)
        if inspection_id is None:
            continue
        violation_code = native(row["violation_code"])
        data.append((inspection_id, violation_code))

    if not data:
        return 0

    # Create a temporary table for the COPY target
    cur.execute("""
        CREATE TEMP TABLE tmp_iv (
            inspection_id BIGINT,
            violation_code TEXT
        ) ON COMMIT DROP;
    """)

    # COPY bulk load into temp table — fastest possible insert method
    with cur.copy("COPY tmp_iv (inspection_id, violation_code) FROM STDIN") as copy:
        for row in data:
            copy.write_row(row)

    # Merge from temp table into the real table, skipping duplicates
    cur.execute("""
        INSERT INTO inspection_violations (inspection_id, violation_code, created_at)
        SELECT inspection_id, violation_code, NOW()
        FROM tmp_iv
        ON CONFLICT (inspection_id, violation_code) DO NOTHING;
    """)

    return len(data)

# =============================================================================
# PROCESS ONE FILE
# =============================================================================

def process_file(file_name: str, cursor_date: date):
    print(f"\n[file] {file_name}")

    # Download and parse CSV
    raw = supabase.storage.from_(BUCKET_NAME).download(file_name)
    df  = pd.read_csv(
        io.BytesIO(raw),
        dtype=DTYPES,
        parse_dates=["inspection_date", "grade_date", "record_date"],
    )

    # Normalize date columns to plain date objects (not datetime)
    for col in ["record_date", "inspection_date", "grade_date"]:
        try:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        except Exception as e:
            print(f"  [warn] Could not parse {col}: {e}")

    # Incremental mode: drop rows already covered by the database cursor
    if FETCH_MODE == "incremental":
        before = len(df)
        df = df[df["record_date"].apply(lambda d: d is not None and d > cursor_date)]
        skipped = before - len(df)
        if skipped:
            print(f"  [filter] Dropped {skipped} rows already in DB (record_date <= {cursor_date})")

    if df.empty:
        print("  [skip] No new rows in this file.")
        return

    print(f"  [rows] {len(df)} rows to process")

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:

            # --- Restaurants ---
            n = bulk_insert_restaurants(cur, df)
            print(f"  [restaurants]          {n} unique (conflicts silently skipped)")

            # --- Violations ---
            n = bulk_insert_violations(cur, df)
            print(f"  [violations]           {n} unique (conflicts silently skipped)")

            # --- Inspections ---
            inspection_ids = bulk_insert_inspections(cur, df)
            print(f"  [inspections]          {len(inspection_ids)} resolved ids")

            # --- Junction table ---
            n = bulk_insert_inspection_violations(cur, df, inspection_ids)
            print(f"  [inspection_violations] {n} rows via COPY (conflicts silently skipped)")

        conn.commit()
        print(f"  [done] Committed.")

# =============================================================================
# MAIN
# =============================================================================

def main():
    cursor_date = get_cursor_date()
    print(f"[cursor] MAX(record_date) in DB: {cursor_date}")
    print(f"[mode]   FETCH_MODE = {FETCH_MODE}")

    files = get_all_files()
    print(f"[files]  {len(files)} CSV files found in '{BUCKET_NAME}'\n")

    if not files:
        print("[done] No files to process.")
        return

    for i, f in enumerate(files, 1):
        print(f"[{i}/{len(files)}]", end=" ")
        process_file(f, cursor_date)

    print("\n[done] All files processed.")


if __name__ == "__main__":
    main()