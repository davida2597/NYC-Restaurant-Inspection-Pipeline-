import pandas as pd


def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Strips leading/trailing whitespace from all string columns."""
    # Support both pandas 2 (object) and pandas 3 (str dtype)
    string_cols = (
        set(df.select_dtypes(include="object").columns)
        | set(df.select_dtypes(include="str").columns)
    )

    for col in string_cols:
        df[col] = df[col].str.strip()

    # Also normalize column names themselves
    df.columns = [c.strip() for c in df.columns]

    return df
