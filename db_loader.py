"""
db_loader.py
------------
Shared database utilities used by both port_data.py (production / Supabase)
and etl.py (Docker / local PostgreSQL).

Contains:
    - SCHEMA_SQL  : the full CREATE TABLE statements for all 4 tables
    - create_schema(conn) : runs SCHEMA_SQL against a live psycopg connection
    - load_dataframe(df, conn) : normalizes a cleaned DataFrame and upserts it
                                 into the 4 tables

Why a separate module?
    port_data.py and etl.py both need to write data into PostgreSQL in exactly
    the same way, but they get the data from different sources (Supabase Storage
    vs. the live API). Putting the loading logic here avoids duplicating ~150
    lines of fiddly SQL in two files.
"""

import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Schema SQL
# Mirrors the schema in setup_db.py so this module is self-contained.
# Tables use IF NOT EXISTS — safe to call multiple times.
# The UNIQUE constraints on inspections and inspection_violations are what make
# ON CONFLICT upserts work correctly; do not remove them.
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
-- One row per unique restaurant, keyed by CAMIS (the city's permanent ID).
CREATE TABLE IF NOT EXISTS restaurants (
    camis               TEXT        PRIMARY KEY,
    dba                 TEXT,
    boro                TEXT,
    building            TEXT,
    street              TEXT,
    zipcode             TEXT,
    phone               TEXT,
    cuisine_description TEXT,
    latitude            NUMERIC(10, 7),
    longitude           NUMERIC(10, 7),
    community_board     TEXT,
    council_district    TEXT,
    census_tract        TEXT,
    bin                 TEXT,
    bbl                 TEXT,
    nta                 TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_restaurants_boro    ON restaurants(boro);
CREATE INDEX IF NOT EXISTS idx_restaurants_zipcode ON restaurants(zipcode);
CREATE INDEX IF NOT EXISTS idx_restaurants_cuisine ON restaurants(cuisine_description);


-- Lookup table: one row per unique violation code across all inspections.
CREATE TABLE IF NOT EXISTS violations (
    violation_code        TEXT PRIMARY KEY,
    violation_description TEXT,
    critical_flag         TEXT,
    created_at            TIMESTAMPTZ DEFAULT NOW()
);


-- One row per unique inspection visit.
-- A visit = one (camis, inspection_date, inspection_type) combination.
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


-- Junction table: which violations were cited in which inspection.
-- One row per (inspection_id, violation_code) pair.
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
# Helpers
# ---------------------------------------------------------------------------

def _to_none(val):
    """
    Convert pandas NA / NaN / empty string to Python None.
    psycopg3 expects None for SQL NULL — it does not accept float('nan').
    """
    if val is None:
        return None
    # pd.isna() handles NaN, NaT, None, and pd.NA
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, str) and val.strip() == "":
        return None
    return val


def _norm_date(val):
    """
    Coerce a value to a datetime.date object (or None).
    Used to build consistent dict keys when looking up inspection IDs.
    """
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return pd.Timestamp(val).date()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_schema(conn):
    """
    Execute SCHEMA_SQL against an open psycopg connection.
    Safe to call repeatedly — all statements use IF NOT EXISTS.
    Commits the transaction before returning.
    """
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    print("[db] Schema created / verified.")


def load_dataframe(df: pd.DataFrame, conn) -> dict:
    """
    Normalize a cleaned inspection DataFrame and load it into PostgreSQL.

    The raw data has one row per violation per inspection (a restaurant with
    3 violations on one visit appears as 3 rows). This function splits that
    flat structure into the 4 normalised tables:

        restaurants          — one row per camis
        violations           — one row per violation_code
        inspections          — one row per (camis, inspection_date, inspection_type)
        inspection_violations — one row per (inspection_id, violation_code)

    All upserts use ON CONFLICT … DO UPDATE / DO NOTHING so this function is
    safely idempotent — calling it twice with the same data is harmless.

    Parameters
    ----------
    df   : cleaned pandas DataFrame (output of clean_data.py pipeline)
    conn : open psycopg3 connection (caller is responsible for closing it)

    Returns
    -------
    dict with row counts: {"restaurants": N, "violations": N, "inspections": N, "iv": N}
    """
    if df.empty:
        print("[load] Empty DataFrame — nothing to load.")
        return {"restaurants": 0, "violations": 0, "inspections": 0, "iv": 0}

    cur = conn.cursor()

    # ------------------------------------------------------------------
    # Step 1 — RESTAURANTS
    # Deduplicate by camis so each restaurant is upserted once.
    # We keep "last" so that if the same camis appears multiple times in
    # the batch, the most recent row's address/name wins.
    # ------------------------------------------------------------------
    restaurant_cols = [
        "camis", "dba", "boro", "building", "street", "zipcode", "phone",
        "cuisine_description", "latitude", "longitude",
        "community_board", "council_district", "census_tract", "bin", "bbl", "nta",
    ]

    restaurants_df = (
        df[restaurant_cols]
        .drop_duplicates(subset=["camis"], keep="last")
        .dropna(subset=["camis"])
    )

    restaurant_rows = [
        {col: _to_none(row[col]) for col in restaurant_cols}
        for _, row in restaurants_df.iterrows()
    ]

    if restaurant_rows:
        cur.executemany(
            """
            INSERT INTO restaurants (
                camis, dba, boro, building, street, zipcode, phone,
                cuisine_description, latitude, longitude,
                community_board, council_district, census_tract, bin, bbl, nta
            ) VALUES (
                %(camis)s, %(dba)s, %(boro)s, %(building)s, %(street)s,
                %(zipcode)s, %(phone)s, %(cuisine_description)s,
                %(latitude)s, %(longitude)s,
                %(community_board)s, %(council_district)s, %(census_tract)s,
                %(bin)s, %(bbl)s, %(nta)s
            )
            ON CONFLICT (camis) DO UPDATE SET
                dba                 = EXCLUDED.dba,
                boro                = EXCLUDED.boro,
                building            = EXCLUDED.building,
                street              = EXCLUDED.street,
                zipcode             = EXCLUDED.zipcode,
                phone               = EXCLUDED.phone,
                cuisine_description = EXCLUDED.cuisine_description,
                latitude            = EXCLUDED.latitude,
                longitude           = EXCLUDED.longitude,
                community_board     = EXCLUDED.community_board,
                council_district    = EXCLUDED.council_district,
                census_tract        = EXCLUDED.census_tract,
                bin                 = EXCLUDED.bin,
                bbl                 = EXCLUDED.bbl,
                nta                 = EXCLUDED.nta,
                updated_at          = NOW()
            """,
            restaurant_rows,
        )

    n_restaurants = len(restaurant_rows)
    print(f"  [restaurants]  upserted {n_restaurants:,}")

    # ------------------------------------------------------------------
    # Step 2 — VIOLATIONS (lookup table)
    # Only rows that actually have a violation_code are relevant here.
    # ------------------------------------------------------------------
    violations_df = (
        df[["violation_code", "violation_description", "critical_flag"]]
        .dropna(subset=["violation_code"])
        .drop_duplicates(subset=["violation_code"], keep="last")
    )

    violation_rows = [
        {
            "violation_code":        _to_none(r["violation_code"]),
            "violation_description": _to_none(r["violation_description"]),
            "critical_flag":         _to_none(r["critical_flag"]),
        }
        for _, r in violations_df.iterrows()
    ]

    if violation_rows:
        cur.executemany(
            """
            INSERT INTO violations (violation_code, violation_description, critical_flag)
            VALUES (%(violation_code)s, %(violation_description)s, %(critical_flag)s)
            ON CONFLICT (violation_code) DO UPDATE SET
                violation_description = EXCLUDED.violation_description,
                critical_flag         = EXCLUDED.critical_flag
            """,
            violation_rows,
        )

    n_violations = len(violation_rows)
    print(f"  [violations]   upserted {n_violations:,}")

    # ------------------------------------------------------------------
    # Step 3 — INSPECTIONS
    # Deduplicate by (camis, inspection_date, inspection_type).
    # ------------------------------------------------------------------
    inspection_cols = [
        "camis", "inspection_date", "inspection_type",
        "action", "score", "grade", "grade_date", "record_date",
    ]

    inspections_df = (
        df[inspection_cols]
        .dropna(subset=["camis"])
        .drop_duplicates(subset=["camis", "inspection_date", "inspection_type"], keep="last")
    )

    inspection_rows = [
        {col: _to_none(row[col]) for col in inspection_cols}
        for _, row in inspections_df.iterrows()
    ]

    if inspection_rows:
        cur.executemany(
            """
            INSERT INTO inspections (
                camis, inspection_date, inspection_type,
                action, score, grade, grade_date, record_date
            ) VALUES (
                %(camis)s, %(inspection_date)s, %(inspection_type)s,
                %(action)s, %(score)s, %(grade)s, %(grade_date)s, %(record_date)s
            )
            ON CONFLICT (camis, inspection_date, inspection_type) DO UPDATE SET
                action      = EXCLUDED.action,
                score       = EXCLUDED.score,
                grade       = EXCLUDED.grade,
                grade_date  = EXCLUDED.grade_date,
                record_date = EXCLUDED.record_date
            """,
            inspection_rows,
        )

    n_inspections = len(inspection_rows)
    print(f"  [inspections]  upserted {n_inspections:,}")

    # ------------------------------------------------------------------
    # Build an (camis, date, type) → inspection_id lookup.
    # We need the auto-generated IDs to populate inspection_violations.
    # Query only the camis values we just inserted to keep it targeted.
    # ------------------------------------------------------------------
    camis_batch = list({r["camis"] for r in inspection_rows if r["camis"]})

    id_map: dict = {}
    if camis_batch:
        cur.execute(
            """
            SELECT id, camis, inspection_date, inspection_type
            FROM inspections
            WHERE camis = ANY(%s)
            """,
            (camis_batch,),
        )
        for row_id, camis, idate, itype in cur.fetchall():
            # idate comes back from psycopg3 as a datetime.date object,
            # so we normalize both sides of the lookup to datetime.date.
            key = (str(camis), idate, str(itype) if itype else None)
            id_map[key] = row_id

    # ------------------------------------------------------------------
    # Step 4 — INSPECTION_VIOLATIONS (junction table)
    # Iterate every row in the original flat data and link each
    # violation_code to its parent inspection via the ID we just looked up.
    # ------------------------------------------------------------------
    iv_rows = []
    for _, row in df.iterrows():
        vc    = _to_none(row.get("violation_code"))
        camis = _to_none(row.get("camis"))
        if vc is None or camis is None:
            continue

        # Build the lookup key using the same date normalization
        idate_key = _norm_date(row.get("inspection_date"))
        itype_val = _to_none(row.get("inspection_type"))
        itype_key = str(itype_val) if itype_val else None

        key       = (str(camis), idate_key, itype_key)
        insp_id   = id_map.get(key)
        if insp_id is None:
            continue  # inspection wasn't inserted (e.g. missing camis) — skip

        iv_rows.append({"inspection_id": insp_id, "violation_code": str(vc)})

    if iv_rows:
        cur.executemany(
            """
            INSERT INTO inspection_violations (inspection_id, violation_code)
            VALUES (%(inspection_id)s, %(violation_code)s)
            ON CONFLICT (inspection_id, violation_code) DO NOTHING
            """,
            iv_rows,
        )

    n_iv = len(iv_rows)
    print(f"  [insp_violations] inserted {n_iv:,}")

    conn.commit()
    cur.close()

    return {
        "restaurants": n_restaurants,
        "violations":  n_violations,
        "inspections": n_inspections,
        "iv":          n_iv,
    }
