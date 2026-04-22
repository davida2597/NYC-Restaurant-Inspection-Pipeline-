import pandas as pd


# All string values that should be treated as missing
NULL_STRINGS = {
    "", "n/a", "na", "null", "none", "nil", "nan",
    "#n/a", "missing", "unknown", "-", "--", "?",
}


def normalize_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Converts empty strings and common null-like sentinels to None (pd.NA)."""
    # select_dtypes(include="str") raises TypeError in pandas 2.x.
    # Object dtype is how pandas 2.x stores string columns.
    string_cols = set(df.select_dtypes(include="object").columns)

    for col in string_cols:
        # Some object columns contain non-string values (e.g. dicts from the
        # API's GeoJSON 'location' field). Skip those silently.
        try:
            df[col] = df[col].where(
                ~df[col].str.strip().str.lower().isin(NULL_STRINGS),
                other=None,
            )
        except (AttributeError, TypeError):
            pass

    return df
