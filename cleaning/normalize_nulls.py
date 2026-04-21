import pandas as pd


# All string values that should be treated as missing
NULL_STRINGS = {
    "", "n/a", "na", "null", "none", "nil", "nan",
    "#n/a", "missing", "unknown", "-", "--", "?",
}


def normalize_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Converts empty strings and common null-like sentinels to None (pd.NA)."""
    # pandas 2.x stores strings as object dtype.
    # select_dtypes(include="str") raises TypeError in pandas 2.x so we only
    # use "object" here.
    string_cols = set(df.select_dtypes(include="object").columns)

    for col in string_cols:
        # Some object-dtype columns contain non-string values (e.g. the
        # 'location' column from the NYC API contains GeoJSON dicts).
        # Calling .str on those raises AttributeError, so we skip them.
        try:
            df[col] = df[col].where(
                ~df[col].str.strip().str.lower().isin(NULL_STRINGS),
                other=None,
            )
        except (AttributeError, TypeError):
            pass

    return df
