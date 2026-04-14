"""
fetch_new.py
------------
Incremental fetch script that downloads only records added or updated in
the NYC DOHMH Restaurant Inspection dataset since the last successful run.

Designed to be triggered automatically by GitHub Actions on a cron schedule.

Usage:
    python fetch_new.py

Requirements in .env:
    API_BASE_URL, API_APP_TOKEN, API_BATCH_SIZE, API_REQUEST_DELAY,
    FETCH_ALL_START_DATE,
    SUPABASE_URL, SUPABASE_KEY, SUPABASE_SECRET_KEY,
    DATABASE_URL, RAW_BUCKET

How the cursor works:
    This script uses record_date (not inspection_date) as its cursor.

    record_date is the date the city's system last touched a row — it is
    updated whenever a record is added OR modified (e.g. a score correction
    or grade change on an old inspection). Using record_date means we catch
    both brand-new inspections AND retroactive updates to existing ones.

    inspection_date only tells us when the inspector physically visited.
    Filtering on inspection_date would silently miss any record that was
    updated after our last run.

    Cursor resolution:
        1. Query MAX(record_date) from the inspections table in the database.
        2. Ask the API for all rows where record_date > that date.
        3. If the database is empty, fall back to FETCH_ALL_START_DATE.

    This approach self-corrects: if a previous run fetched data but
    port_data.py crashed before inserting it, the MAX(record_date) will
    still reflect the last successfully inserted date, and the next run
    will re-fetch the missing window automatically.

Output:
    Saves a CSV to Supabase Storage (raw bucket) named:
        {cursor_date+1day}_{today}.csv

    Also writes the output filename to the environment file
    GITHUB_ENV (when running in GitHub Actions) so downstream
    steps (clean_data.py, port_data.py) know which file to process.

    Locally, the filename is printed to stdout and written to
    last_fetch.txt for manual chaining of scripts.
"""

import io
import os
import time
from datetime import date, datetime, timedelta

import pandas as pd
import psycopg
import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# ---------------------------------------------------------------------------
# Config from .env
# ---------------------------------------------------------------------------
API_BASE_URL        = os.environ["API_BASE_URL"]
API_APP_TOKEN       = os.environ["API_APP_TOKEN"]
API_BATCH_SIZE      = int(os.environ.get("API_BATCH_SIZE", 1000))
API_REQUEST_DELAY   = float(os.environ.get("API_REQUEST_DELAY", 0.5))
FETCH_ALL_START_DATE = os.environ.get("FETCH_ALL_START_DATE", "2010-01-01")
RAW_BUCKET          = os.environ.get("RAW_BUCKET", "raw")

SUPABASE_URL        = os.environ["SUPABASE_URL"]
SUPABASE_KEY        = os.environ["SUPABASE_KEY"]       # publishable key is fine for storage writes
DATABASE_URL        = os.environ["DATABASE_URL"]       # needed to query MAX(record_date)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------------------------------------------------------------------
# Cursor — determine the "since" date
# ---------------------------------------------------------------------------

def get_cursor_date() -> date:
    """
    Return the most recent record_date already stored in the database.
    Falls back to FETCH_ALL_START_DATE if the table is empty or unreachable.

    We connect directly via psycopg3 rather than the Supabase client because
    MAX() aggregation is simpler and more reliable over a direct SQL connection.
    """
    try:
        conn   = psycopg.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(record_date) FROM inspections;")
        result = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        if result is not None:
            # result is a datetime.date object from psycopg3
            max_date = result if isinstance(result, date) else result.date()
            print(f"[cursor] MAX(record_date) in database : {max_date}")
            return max_date

    except Exception as e:
        print(f"[cursor] Could not query database: {e}")

    fallback = date.fromisoformat(FETCH_ALL_START_DATE)
    print(f"[cursor] Database empty or unreachable — falling back to {fallback}")
    return fallback


# ---------------------------------------------------------------------------
# API fetching
# ---------------------------------------------------------------------------

def fetch_since(since: date, until: date) -> pd.DataFrame:
    """
    Fetch all rows where record_date is strictly after `since` and up to
    `until` (inclusive), paging through API_BATCH_SIZE rows at a time.

    We use a strict greater-than (>) on record_date so we never re-fetch
    the last batch of records already in the database.

    $where filter used:
        record_date > 'YYYY-MM-DDT23:59:59'
        AND record_date <= 'YYYY-MM-DDT23:59:59'
    """
    all_rows = []
    offset   = 0
    page     = 1

    # Strictly after the cursor date, up to and including today
    where_clause = (
        f"record_date > '{since}T23:59:59' AND "
        f"record_date <= '{until}T23:59:59'"
    )

    print(f"[fetch] Requesting rows where {where_clause}")

    while True:
        params = {
            "$limit":      API_BATCH_SIZE,
            "$offset":     offset,
            "$order":      "record_date ASC, camis ASC",
            "$where":      where_clause,
            "$$app_token": API_APP_TOKEN,
        }

        try:
            resp = requests.get(API_BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"  [error] Request failed on page {page}: {e}")
            raise

        rows = resp.json()

        if not rows:
            break

        all_rows.extend(rows)
        print(f"  page {page:>3} — fetched {len(rows):>5} rows  "
              f"(running total: {len(all_rows):>6})")

        if len(rows) < API_BATCH_SIZE:
            break

        offset += API_BATCH_SIZE
        page   += 1
        time.sleep(API_REQUEST_DELAY)

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


# ---------------------------------------------------------------------------
# Storage upload
# ---------------------------------------------------------------------------

def upload_csv(df: pd.DataFrame, filename: str):
    """Upload DataFrame as CSV to the raw bucket in Supabase Storage."""
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    try:
        supabase.storage.from_(RAW_BUCKET).update(
            filename, csv_bytes, {"content-type": "text/csv"}
        )
    except Exception:
        supabase.storage.from_(RAW_BUCKET).upload(
            filename, csv_bytes, {"content-type": "text/csv"}
        )
    print(f"[upload] raw/{filename}  ({len(df)} rows)")


# ---------------------------------------------------------------------------
# Pass filename to downstream steps
# ---------------------------------------------------------------------------

def export_filename(filename: str):
    """
    Make the output filename available to downstream pipeline steps.

    In GitHub Actions: writes TARGET_FILE=filename to $GITHUB_ENV so
    clean_data.py and port_data.py can read it as an environment variable
    in subsequent workflow steps.

    Locally: writes the filename to last_fetch.txt so you can inspect it,
    and prints it clearly to stdout.
    """
    # GitHub Actions environment
    github_env = os.environ.get("GITHUB_ENV")
    if github_env:
        with open(github_env, "a") as f:
            f.write(f"TARGET_FILE={filename}\n")
        print(f"[env] Wrote TARGET_FILE={filename} to $GITHUB_ENV")

    # Local fallback
    with open("last_fetch.txt", "w") as f:
        f.write(filename)
    print(f"[env] Wrote filename to last_fetch.txt")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    today  = date.today()
    cursor = get_cursor_date()

    print(f"\n{'='*60}")
    print(f"  fetch_new.py")
    print(f"  Cursor (last record_date in DB) : {cursor}")
    print(f"  Fetching records up to          : {today}")
    print(f"  Batch size                      : {API_BATCH_SIZE} rows per request")
    print(f"{'='*60}\n")

    # Nothing to do if cursor is already today
    if cursor >= today:
        print("[done] Database is already up to date. Nothing to fetch.")
        return

    # Fetch new records
    try:
        df = fetch_since(since=cursor, until=today)
    except Exception as e:
        print(f"\n[error] Fetch failed: {e}")
        raise SystemExit(1)

    # Exit cleanly if the API returned nothing new
    if df.empty:
        print(f"\n[done] API returned 0 new records since {cursor}. "
              f"Database is up to date.")
        return

    # Actual date range of the fetched data (from what the API returned)
    # Use the data's own record_date range for the filename, not today's date
    try:
        fetched_min = pd.to_datetime(df["record_date"]).dt.date.min()
        fetched_max = pd.to_datetime(df["record_date"]).dt.date.max()
    except Exception:
        # Fallback if record_date column is missing or unparseable
        fetched_min = cursor + timedelta(days=1)
        fetched_max = today

    filename = f"{fetched_min}_{fetched_max}.csv"

    print(f"\n[summary]")
    print(f"  Records fetched   : {len(df)}")
    print(f"  record_date range : {fetched_min} → {fetched_max}")
    print(f"  Output filename   : {filename}\n")

    # Upload to Supabase Storage
    upload_csv(df, filename)

    # Pass filename to downstream steps
    export_filename(filename)

    print(f"\n[done] fetch_new.py complete.")
    print(f"  Next: run clean_data.py with TARGET_FILE={filename}")


if __name__ == "__main__":
    main()
