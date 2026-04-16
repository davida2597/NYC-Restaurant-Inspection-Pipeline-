import pandas as pd

EXPECTED_COLUMNS = [
    "camis",
    "dba",
    "boro",
    "building",
    "street",
    "zipcode",
    "phone",
    "cuisine_description",
    "inspection_date",
    "action",
    "violation_code",
    "violation_description",
    "critical_flag",
    "score",
    "grade",
    "grade_date",
    "record_date",
    "inspection_type",
    "latitude",
    "longitude",
    "community_board",
    "council_district",
    "census_tract",
    "bin",
    "bbl",
    "nta",
    "location",
]

def enforce_column_layout(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforces strict column layout:
    - Adds missing columns
    - Reorders columns exactly as specified
    - Fills missing values with pd.NA
    """

    df = df.copy()

    # Add missing columns
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    # Reorder columns
    df = df[EXPECTED_COLUMNS]

    return df