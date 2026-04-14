import pandas as pd


# All string values that should be treated as missing
NULL_STRINGS = {
    "", "n/a", "na", "null", "none", "nil", "nan",
    "#n/a", "missing", "unknown", "-", "--", "?",
}


def normalize_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Converts empty strings and common null-like sentinels to None (pd.NA)."""
    # Support both pandas 2 (object) and pandas 3 (str dtype)
    string_cols = (
        set(df.select_dtypes(include="object").columns)
        | set(df.select_dtypes(include="str").columns)
    )

    for col in string_cols:
        df[col] = df[col].where(
            ~df[col].str.strip().str.lower().isin(NULL_STRINGS),
            other=None,
        )

    return df
