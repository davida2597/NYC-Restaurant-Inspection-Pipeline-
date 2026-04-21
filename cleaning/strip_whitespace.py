import pandas as pd


def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Strips leading/trailing whitespace from all string columns."""
    # pandas 2.x stores strings as object dtype.
    # select_dtypes(include="str") raises TypeError in pandas 2.x so we only
    # use "object" here.
    string_cols = set(df.select_dtypes(include="object").columns)

    for col in string_cols:
        # Guard: object columns may contain non-string values (e.g. dicts
        # from the API's 'location' GeoJSON field). Skip those silently.
        try:
            df[col] = df[col].str.strip()
        except AttributeError:
            pass

    # Also normalize column names themselves
    df.columns = [c.strip() for c in df.columns]

    return df
