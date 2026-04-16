import pandas as pd

def validate_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforces strict column types and removes invalid rows.

    Rules:
    - Column order must match expected schema
    - Type mismatches -> row dropped
    - NA allowed everywhere
    """

    df = df.copy()

    # -----------------------------
    # Expected schema (by position)
    # -----------------------------
    expected_types = [
        "int", "str", "str", "str", "str",
        "int", "int", "str",
        "date",
        "str", "str", "str", "str",
        "int",
        "char",
        "date", "date",
        "str",
        "float", "float",
        "int", "int", "int", "int", "int",
        "str", "str"
    ]

    if len(df.columns) != len(expected_types):
        raise ValueError("Column count does not match expected schema")

    # Force datetime parsing for date columns
    date_indices = [i for i, t in enumerate(expected_types) if t == "date"]
    for i in date_indices:
        col = df.columns[i]
        df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce")


    # -----------------------------
    # Row validation mask
    # -----------------------------
    valid_mask = pd.Series(True, index=df.index)

    for i, (col, typ) in enumerate(zip(df.columns, expected_types)):

        series = df[col]

        # -------------------------
        # Type checks
        # -------------------------
        if typ == "int":
            coerced = pd.to_numeric(series, errors="coerce")
            valid_mask &= coerced.notna() | series.isna()

        elif typ == "float":
            coerced = pd.to_numeric(series, errors="coerce")
            valid_mask &= coerced.notna() | series.isna()

        elif typ == "str":
            valid_mask &= series.isna() | series.astype(str).apply(lambda x: isinstance(x, str))

        elif typ == "char":
            valid_mask &= series.isna() | series.astype(str).apply(lambda x: len(str(x)) == 1)

        elif typ == "date":
            valid_mask &= series.isna() | pd.to_datetime(series, errors="coerce").notna()

    # -----------------------------
    # Apply filter
    # -----------------------------
    cleaned_df = df[valid_mask].copy()

    return cleaned_df