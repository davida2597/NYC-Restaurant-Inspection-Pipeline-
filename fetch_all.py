"""
fetch_all.py
------------
One-time script that downloads the complete history of the NYC DOHMH
Restaurant Inspection Results dataset in batches of 1000 rows, saving
each date-window as a CSV in Supabase Storage (raw bucket).

Usage:
    python fetch_all.py

Requirements in .env:
    API_BASE_URL, API_APP_TOKEN, API_BATCH_SIZE, API_REQUEST_DELAY,
    FETCH_ALL_START_DATE, FETCH_BATCH_DAYS,
    SUPABASE_URL, SUPABASE_KEY, RAW_BUCKET

Crash recovery:
    Progress is saved to Supabase Storage as 'progress.json' after every
    successfully completed date window. Re-running the script resumes from
    the last completed window automatically.
"""

import io
import json
import os
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# ---------------------------------------------------------------------------
# Config from .env
# ---------------------------------------------------------------------------
API_BASE_URL      = os.environ["API_BASE_URL"]
API_APP_TOKEN     = os.environ["API_APP_TOKEN"]
API_BATCH_SIZE    = int(os.environ.get("API_BATCH_SIZE", 1000))
API_REQUEST_DELAY = float(os.environ.get("API_REQUEST_DELAY", 0.5))
FETCH_BATCH_DAYS  = int(os.environ.get("FETCH_BATCH_DAYS", 90))
FETCH_START_DATE  = os.environ.get("FETCH_ALL_START_DATE", "2010-01-01")
RAW_BUCKET        = os.environ.get("RAW_BUCKET", "raw")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

PROGRESS_FILE = "progress.json"

# ---------------------------------------------------------------------------
# Date window utilities
# ---------------------------------------------------------------------------

def generate_windows(start: date, end: date, days: int):
    """Yield (window_start, window_end) tuples covering start..end."""
    current = start
    while current < end:
        window_end = min(current + timedelta(days=days - 1), end)
        yield current, window_end
        current = window_end + timedelta(days=1)


def window_filename(start: date, end: date) -> str:
    return f"{start}_{end}.csv"

# ---------------------------------------------------------------------------
# Progress tracking (stored as JSON in Supabase Storage)
# ---------------------------------------------------------------------------

def load_progress() -> set:
    """Return a set of already-completed window filenames."""
    try:
        data = supabase.storage.from_(RAW_BUCKET).download(PROGRESS_FILE)
        completed = json.loads(data.decode("utf-8"))
        print(f"[resume] Found {len(completed)} completed windows in progress.json")
        return set(completed)
    except Exception:
        print("[resume] No progress file found — starting from scratch")
        return set()


def save_progress(completed: set):
    """Upload progress.json to Supabase Storage."""
    payload = json.dumps(sorted(completed)).encode("utf-8")
    try:
        supabase.storage.from_(RAW_BUCKET).update(
            PROGRESS_FILE, payload, {"content-type": "application/json"}
        )
    except Exception:
        supabase.storage.from_(RAW_BUCKET).upload(
            PROGRESS_FILE, payload, {"content-type": "application/json"}
        )

# ---------------------------------------------------------------------------
# API fetching
# ---------------------------------------------------------------------------

def fetch_window(start: date, end: date) -> pd.DataFrame:
    """
    Fetch all inspection records whose inspection_date falls within
    [start, end], paging through 1000 rows at a time.

    The API date filter uses SoQL:
        $where=inspection_date >= 'YYYY-MM-DDT00:00:00' AND
               inspection_date <= 'YYYY-MM-DDT23:59:59'
    """
    all_rows = []
    offset   = 0
    page     = 1

    where_clause = (
        f"inspection_date >= '{start}T00:00:00' AND "
        f"inspection_date <= '{end}T23:59:59'"
    )

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
            print(f"  [error] Request failed on page {page}: {e}")
            raise

        rows = resp.json()

        if not rows:
            break

        all_rows.extend(rows)
        print(f"  page {page:>3} — fetched {len(rows):>5} rows "
              f"(running total: {len(all_rows):>6})")

        # If we got fewer rows than the batch size, we've hit the last page
        if len(rows) < API_BATCH_SIZE:
            break

        offset += API_BATCH_SIZE
        page   += 1
        time.sleep(API_REQUEST_DELAY)

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def upload_csv(df: pd.DataFrame, filename: str):
    """Upload a DataFrame as CSV to the raw bucket in Supabase Storage."""
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    try:
        # Try update first (file might exist from a previous partial attempt)
        supabase.storage.from_(RAW_BUCKET).update(
            filename, csv_bytes, {"content-type": "text/csv"}
        )
    except Exception:
        supabase.storage.from_(RAW_BUCKET).upload(
            filename, csv_bytes, {"content-type": "text/csv"}
        )
    print(f"  [upload] raw/{filename} ({len(df)} rows)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    overall_start = date.fromisoformat(FETCH_START_DATE)
    overall_end   = date.today()

    all_windows   = list(generate_windows(overall_start, overall_end, FETCH_BATCH_DAYS))
    completed     = load_progress()
    total         = len(all_windows)
    remaining     = [w for w in all_windows if window_filename(*w) not in completed]

    print(f"\n{'='*60}")
    print(f"  fetch_all.py")
    print(f"  Dataset  : NYC Restaurant Inspections (43nn-pn8j)")
    print(f"  Range    : {overall_start} → {overall_end}")
    print(f"  Windows  : {total} total, {len(remaining)} remaining")
    print(f"  Batch sz : {API_BATCH_SIZE} rows per API request")
    print(f"{'='*60}\n")

    if not remaining:
        print("All windows already completed. Nothing to do.")
        return

    for i, (win_start, win_end) in enumerate(remaining, 1):
        filename = window_filename(win_start, win_end)
        pct      = (total - len(remaining) + i) / total * 100

        print(f"[{i}/{len(remaining)}]  {win_start} → {win_end}  ({pct:.1f}% overall)")

        try:
            df = fetch_window(win_start, win_end)
        except Exception as e:
            print(f"  [skip] Failed to fetch window {filename}: {e}")
            print(f"  Re-run the script to retry this window.\n")
            continue

        if df.empty:
            print(f"  [skip] No records in this date range — marking complete\n")
        else:
            upload_csv(df, filename)

        completed.add(filename)
        save_progress(completed)
        print()

    print(f"{'='*60}")
    print(f"  Done. {len(completed)}/{total} windows complete.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
