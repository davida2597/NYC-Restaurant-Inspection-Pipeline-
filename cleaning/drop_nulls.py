import pandas as pd


# Add the column names that must be present for a row to be considered valid.
# If left empty, the function drops rows where ALL columns are null.
REQUIRED_COLUMNS: list[str] = ['camis', 'dba']  # These fields must not be null


def drop_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Drops rows where required fields are null.

    If REQUIRED_COLUMNS is populated, only those columns are checked.
    Otherwise, rows that are entirely null are dropped.
    """
    before = len(df)

    required = [c for c in REQUIRED_COLUMNS if c in df.columns]
    if required:
        df = df.dropna(subset=required)
    else:
        df = df.dropna(how="all")

    dropped = before - len(df)
    if dropped:
        print(f"  drop_nulls: removed {dropped} rows with null required fields")

    return df
