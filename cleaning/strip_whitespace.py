import pandas as pd


def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Strips leading/trailing whitespace from all string columns."""
    # select_dtypes(include="str") raises TypeError in pandas 2.x.
    # Object dtype is how pandas 2.x stores string columns.
    string_cols = set(df.select_dtypes(include="object").columns)

    for col in string_cols:
        # Guard: some object columns contain non-string values (e.g. dicts).
        try:
            df[col] = df[col].str.strip()
        except AttributeError:
            pass

    # Also normalize column names themselves
    df.columns = [c.strip() for c in df.columns]

    return df
