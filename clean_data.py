# clean_data_parallel.py

import os
import io
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Cleaning functions
from cleaning.validate_types import validate_types
from cleaning.strip_whitespace import strip_whitespace
from cleaning.remove_duplicates import remove_duplicates
from cleaning.parse_dates import parse_dates
from cleaning.normalize_nulls import normalize_nulls
from cleaning.normalize_ids import normalize_ids
from cleaning.normalize_caps import normalize_caps
from cleaning.drop_nulls import drop_nulls

# -----------------------------
# ENV + CLIENT
# -----------------------------
load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

RAW_BUCKET = "raw_data_csv"
CLEAN_BUCKET = "cleaned_data_csv"

CLEAN_MODE = "all"  # or "recent"
RECENT_HOURS = 23
MAX_WORKERS = 5

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------
# PIPELINE
# -----------------------------
CLEANING_PIPELINE = [
    normalize_nulls,
    strip_whitespace,
    normalize_caps,
    parse_dates,
    validate_types,
    normalize_ids,
    drop_nulls,
    remove_duplicates,
]

# -----------------------------
# HELPERS
# -----------------------------
def list_files(bucket):
    return supabase.storage.from_(bucket).list()


def is_recent(file_meta):
    if CLEAN_MODE == "all":
        return True

    created = file_meta.get("created_at") or file_meta.get("updated_at")
    if not created:
        return False

    created_time = datetime.fromisoformat(created.replace("Z", "+00:00"))
    cutoff = datetime.now(timezone.utc) - timedelta(hours=RECENT_HOURS)

    return created_time >= cutoff


def file_exists_clean(bucket, filename):
    files = supabase.storage.from_(bucket).list()
    return filename in {f["name"] for f in files}


def download_csv(filename):
    response = supabase.storage.from_(RAW_BUCKET).download(filename)
    return pd.read_csv(io.BytesIO(response))


def upload_csv(df, filename):
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    supabase.storage.from_(CLEAN_BUCKET).upload(
        filename,
        csv_bytes,
        {"upsert": "true"},
    )


# -----------------------------
# CLEANING CORE
# -----------------------------
def run_pipeline(df):
    for func in CLEANING_PIPELINE:
        df = func(df)
    return df


def process_file(filename):
    """Full ETL for one file."""
    try:
        print(f"[START] {filename}")

        df = download_csv(filename)
        df = run_pipeline(df)
        upload_csv(df, filename)

        print(f"[DONE] {filename}")

        return {"file": filename, "status": "success"}

    except Exception as e:
        print(f"[ERROR] {filename} → {e}")
        return {"file": filename, "status": "failed", "error": str(e)}


# -----------------------------
# MAIN PARALLEL ENGINE
# -----------------------------
def main():

    print("\n[1] Listing files...")
    files = list_files(RAW_BUCKET)

    csv_files = [
        f for f in files
        if f["name"].endswith(".csv") and is_recent(f)
    ]

    print(f"[2] Eligible files: {len(csv_files)}")

    selected = []

    for f in csv_files:
        name = f["name"]

        if file_exists_clean(CLEAN_BUCKET, name):
            continue

        selected.append(name)

    print(f"[3] Files to process: {len(selected)}")
    print(f"[4] Running with {MAX_WORKERS} workers...\n")

    results = []

    # -----------------------------
    # PARALLEL EXECUTION
    # -----------------------------
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_file, f): f for f in selected}

        for future in as_completed(futures):
            result = future.result()
            results.append(result)

    # -----------------------------
    # SUMMARY
    # -----------------------------
    success = len([r for r in results if r["status"] == "success"])
    failed = len(results) - success

    print("\n========== SUMMARY ==========")
    print(f"Total processed: {len(results)}")
    print(f"Success: {success}")
    print(f"Failed: {failed}")


if __name__ == "__main__":
    main()