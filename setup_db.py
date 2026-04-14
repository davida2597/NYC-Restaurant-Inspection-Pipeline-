"""
setup_db.py
-----------
One-time script that creates the normalized PostgreSQL schema in Supabase.
Run this once before running port_data.py for the first time.

Usage:
    python setup_db.py

Requirements in .env:
    DATABASE_URL, SUPABASE_URL, SUPABASE_KEY, RAW_BUCKET, CLEANED_BUCKET

Schema design:
    The raw dataset has one row per violation per inspection. This means a
    single inspection visit can appear as many rows (one per violation found).
    We normalize this into four tables:

        restaurants     — one row per unique restaurant (keyed by camis)
        inspections     — one row per unique inspection visit
                          (keyed by camis + inspection_date + inspection_type)
        violations      — one row per violation code (lookup table)
        inspection_violations — junction: which violations appeared in which
                          inspection (one row per violation per inspection)

    This eliminates the massive redundancy in the raw data where restaurant
    name, address, borough, and cuisine are repeated on every violation row.

    The Supabase Storage buckets (raw, cleaned) are also created here if
    they do not already exist.
"""

import os
import psycopg
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

DATABASE_URL   = os.environ["DATABASE_URL"]
SUPABASE_URL   = os.environ["SUPABASE_URL"]
SUPABASE_KEY   = os.environ["SUPABASE_KEY"]
RAW_BUCKET     = os.environ.get("RAW_BUCKET", "raw_data_csv")
CLEANED_BUCKET = os.environ.get("CLEANED_BUCKET", "cleaned_data_csv")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------------------------------------------------------------------
# SQL — schema definition
# ---------------------------------------------------------------------------
# All tables use IF NOT EXISTS so this script is safely re-runnable.
# The unique constraints on each table are what enable ON CONFLICT DO NOTHING
# in port_data.py — do not remove them.

SCHEMA_SQL = """
-- ============================================================
-- restaurants
-- One row per unique establishment, identified by CAMIS number.
-- CAMIS is the city's permanent ID for a restaurant — it does
-- not change even if the restaurant is renamed or remodelled.
-- ============================================================
CREATE TABLE IF NOT EXISTS restaurants (
    camis            TEXT        PRIMARY KEY,
    dba              TEXT,                       -- "doing business as" name
    boro             TEXT,
    building         TEXT,
    street           TEXT,
    zipcode          TEXT,
    phone            TEXT,
    cuisine_description TEXT,
    latitude         NUMERIC(10, 7),
    longitude        NUMERIC(10, 7),
    community_board  TEXT,
    council_district TEXT,
    census_tract     TEXT,
    bin              TEXT,
    bbl              TEXT,
    nta              TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_restaurants_boro    ON restaurants(boro);
CREATE INDEX IF NOT EXISTS idx_restaurants_zipcode ON restaurants(zipcode);
CREATE INDEX IF NOT EXISTS idx_restaurants_cuisine ON restaurants(cuisine_description);


-- ============================================================
-- violations
-- Lookup table for violation codes and their descriptions.
-- One row per unique violation code.
-- ============================================================
CREATE TABLE IF NOT EXISTS violations (
    violation_code        TEXT PRIMARY KEY,
    violation_description TEXT,
    critical_flag         TEXT,   -- 'Critical', 'Not Critical', 'Not Applicable'
    created_at            TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================
-- inspections
-- One row per unique inspection visit.
-- A visit is uniquely identified by (camis, inspection_date,
-- inspection_type) — a restaurant can have multiple inspection
-- types on the same date (e.g. initial + re-inspection).
-- ============================================================
CREATE TABLE IF NOT EXISTS inspections (
    id              BIGSERIAL   PRIMARY KEY,
    camis           TEXT        NOT NULL REFERENCES restaurants(camis),
    inspection_date DATE,
    inspection_type TEXT,
    action          TEXT,
    score           SMALLINT,
    grade           TEXT,
    grade_date      DATE,
    record_date     DATE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (camis, inspection_date, inspection_type)
);

CREATE INDEX IF NOT EXISTS idx_inspections_camis           ON inspections(camis);
CREATE INDEX IF NOT EXISTS idx_inspections_inspection_date ON inspections(inspection_date);
CREATE INDEX IF NOT EXISTS idx_inspections_grade           ON inspections(grade);
CREATE INDEX IF NOT EXISTS idx_inspections_score           ON inspections(score);


-- ============================================================
-- inspection_violations
-- Junction table linking each inspection to the violations
-- that were cited during that visit.
-- One row per (inspection_id, violation_code) pair.
-- ============================================================
CREATE TABLE IF NOT EXISTS inspection_violations (
    id             BIGSERIAL PRIMARY KEY,
    inspection_id  BIGINT    NOT NULL REFERENCES inspections(id),
    violation_code TEXT      REFERENCES violations(violation_code),
    created_at     TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE (inspection_id, violation_code)
);

CREATE INDEX IF NOT EXISTS idx_iv_inspection_id  ON inspection_violations(inspection_id);
CREATE INDEX IF NOT EXISTS idx_iv_violation_code ON inspection_violations(violation_code);
"""

# ---------------------------------------------------------------------------
# Storage bucket creation
# ---------------------------------------------------------------------------

def ensure_bucket(bucket_name: str):
    """Create a Supabase Storage bucket if it doesn't already exist."""
    existing = [b.name for b in supabase.storage.list_buckets()]    
    if bucket_name in existing:
        print(f"  [storage] Bucket '{bucket_name}' already exists — skipping")
        return
    supabase.storage.create_bucket(bucket_name, options={"public": False})
    print(f"  [storage] Created bucket '{bucket_name}'")


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------

def create_schema():
    """Connect to PostgreSQL and execute the schema SQL."""
    print("  [db] Connecting to Supabase PostgreSQL...")
    conn   = psycopg.connect(DATABASE_URL)
    cursor = conn.cursor()

    print("  [db] Running schema SQL...")
    cursor.execute(SCHEMA_SQL)
    conn.commit()

    # Report what now exists
    cursor.execute("""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY tablename;
    """)
    tables = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return tables


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"\n{'='*60}")
    print(f"  setup_db.py")
    print(f"  Supabase project : {SUPABASE_URL}")
    print(f"{'='*60}\n")

    # 1. Create Storage buckets
    print("[1/2] Ensuring Supabase Storage buckets exist...")
    ensure_bucket(RAW_BUCKET)
    ensure_bucket(CLEANED_BUCKET)
    print()

    # 2. Create database schema
    print("[2/2] Creating database schema...")
    try:
        tables = create_schema()
        print(f"\n  [db] Schema applied successfully.")
        print(f"  [db] Tables present in public schema:")
        for t in tables:
            print(f"       - {t}")
    except Exception as e:
        print(f"\n  [error] Schema creation failed: {e}")
        raise

    print(f"\n{'='*60}")
    print(f"  Setup complete.")
    print(f"  Next steps:")
    print(f"    1. Run fetch_all.py  — downloads raw CSVs to Supabase Storage")
    print(f"    2. Run clean_data.py — cleans each raw CSV")
    print(f"    3. Run port_data.py  — loads cleaned CSVs into the database")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
