"""
port_data.py
------------
PRODUCTION script: reads cleaned CSVs from Supabase Storage and loads them
into the normalized PostgreSQL schema (4 tables).

This is step 3 in the Supabase-hosted pipeline:
    fetch_all.py / fetch_new.py  →  clean_data.py  →  port_data.py

Run modes
---------
1. GitHub Actions (incremental, single file):
   The upstream clean_data.py step writes TARGET_FILE=<filename> into
   $GITHUB_ENV.  This script reads that variable and processes only that one
   file — fast, cheap, no duplicate work.

2. Manual / backfill (all files):
   Unset TARGET_FILE (or set it to an empty string) and the script will walk
   every CSV in the cleaned bucket that has not yet been ported.
   Progress is tracked via a 'ported_files.json' file stored in the cleaned
   bucket so you can safely interrupt and re-run.

Usage
-----
    python port_data.py

Requirements in .env
--------------------
    SUPABASE_URL, SUPABASE_KEY (or SUPABASE_SECRET_KEY for DB writes),
    DATABASE_URL, CLEANED_BUCKET
"""

import io
import json
import os
import time

import pandas as pd
import psycopg
from dotenv import load_dotenv
from supabase import create_client

from db_loader import create_schema, load_dataframe

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SUPABASE_URL   = os.environ["SUPABASE_URL"]
# Use the secret key here — port_data.py writes to the database, which
# requires full privileges (bypasses Supabase Row Level Security).
SUPABASE_KEY   = os.environ.get("SUPABASE_SECRET_KEY") or os.environ["SUPABASE_KEY"]
DATABASE_URL   = os.environ["DATABASE_URL"]
CLEANED_BUCKET = os.environ.get("CLEANED_BUCKET", "cleaned_data_csv")

# If GitHub Actions set TARGET_FILE, process only that file.
# Otherwise, port everything in the cleaned bucket.
TARGET_FILE    = os.environ.get("TARGET_FILE", "").strip()

PROGRESS_FILE  = "ported_files.json"  # stored inside the cleaned bucket

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------------------------------------------------------------------------
# Progress tracking (stored in Supabase Storage so it survives restarts)
# ---------------------------------------------------------------------------

def load_progress() -> set:
    """Return a set of filenames already successfully ported to PostgreSQL."""
    try:
        data = supabase.storage.from_(CLEANED_BUCKET).download(PROGRESS_FILE)
        completed = json.loads(data.decode("utf-8"))
        print(f"[resume] {len(completed)} files already ported (from {PROGRESS_FILE})")
        return set(completed)
    except Exception:
        print("[resume] No progress file found — starting fresh")
        return set()


def save_progress(completed: set):
    """Upload progress.json back to the cleaned bucket after each file."""
    payload = json.dumps(sorted(completed)).encode("utf-8")
    try:
        supabase.storage.from_(CLEANED_BUCKET).update(
            PROGRESS_FILE, payload, {"content-type": "application/json"}
        )
    except Exception:
        supabase.storage.from_(CLEANED_BUCKET).upload(
            PROGRESS_FILE, payload, {"content-type": "application/json"}
        )


# ---------------------------------------------------------------------------
# Supabase Storage helpers
# ---------------------------------------------------------------------------

def list_cleaned_files() -> list[str]:
    """Return all .csv filenames in the cleaned bucket."""
    files = supabase.storage.from_(CLEANED_BUCKET).list()
    return [f["name"] for f in files if f["name"].endswith(".csv")]


def download_cleaned_csv(filename: str) -> pd.DataFrame:
    """Download a cleaned CSV from Supabase Storage into a DataFrame."""
    response = supabase.storage.from_(CLEANED_BUCKET).download(filename)
    return pd.read_csv(io.BytesIO(response))


# ---------------------------------------------------------------------------
# Core ETL per file
# ---------------------------------------------------------------------------

def port_file(filename: str, conn) -> dict:
    """
    Download one cleaned CSV and load it into PostgreSQL.

    Parameters
    ----------
    filename : name of the CSV inside the cleaned bucket
    conn     : open psycopg3 connection (shared across all files in a run)

    Returns
    -------
    dict with status and row counts
    """
    print(f"\n[START] {filename}")
    start = time.time()

    try:
        # 1. Download from Supabase Storage
        df = download_cleaned_csv(filename)
        print(f"  Downloaded {len(df):,} rows")

        # 2. Load into the 4 normalised tables
        counts = load_dataframe(df, conn)

        elapsed = time.time() - start
        print(f"[DONE]  {filename}  ({elapsed:.1f}s)")
        return {"file": filename, "status": "success", **counts}

    except Exception as e:
        print(f"[ERROR] {filename} → {e}")
        return {"file": filename, "status": "failed", "error": str(e)}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"\n{'='*60}")
    print("  port_data.py — Supabase Storage → PostgreSQL")
    print(f"{'='*60}\n")

    # Open one persistent database connection for the whole run.
    # This is more efficient than opening a new connection per file.
    print("[db] Connecting to PostgreSQL …")
    conn = psycopg.connect(DATABASE_URL)
    print("[db] Connected.")

    # Make sure the schema exists before we try to write anything
    create_schema(conn)

    # ------------------------------------------------------------------
    # Determine which file(s) to process
    # ------------------------------------------------------------------
    if TARGET_FILE:
        # GitHub Actions mode: a single file was passed via $GITHUB_ENV
        print(f"\n[mode] Single-file mode — processing: {TARGET_FILE}\n")
        result = port_file(TARGET_FILE, conn)
        results = [result]

    else:
        # Batch / backfill mode: process everything not yet ported
        print("[mode] Batch mode — scanning cleaned bucket …")
        all_files = list_cleaned_files()
        completed = load_progress()

        to_port = [f for f in all_files if f not in completed]

        print(f"  Total CSVs in bucket : {len(all_files)}")
        print(f"  Already ported       : {len(completed)}")
        print(f"  Remaining            : {len(to_port)}\n")

        results = []
        for i, filename in enumerate(to_port, 1):
            print(f"[{i}/{len(to_port)}] {filename}")
            result = port_file(filename, conn)
            results.append(result)

            if result["status"] == "success":
                completed.add(filename)
                save_progress(completed)

            # Small delay to be polite to Supabase rate limits
            time.sleep(0.3)

    conn.close()

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    success = [r for r in results if r.get("status") == "success"]
    failed  = [r for r in results if r.get("status") == "failed"]

    print(f"\n{'='*60}")
    print(f"  SUMMARY")
    print(f"  Files processed : {len(results)}")
    print(f"  Success         : {len(success)}")
    print(f"  Failed          : {len(failed)}")

    if failed:
        print("\n  Failed files:")
        for r in failed:
            print(f"    - {r['file']} : {r.get('error')}")

    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
