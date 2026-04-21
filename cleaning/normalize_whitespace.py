import pandas as pd

def normalize_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove accidental double/multiple whitespace inside every string cell
    by tokenizing on whitespace and rejoining with a single space.
    """

    df = df.copy()

    # Only operate on string/object columns
    str_cols = df.select_dtypes(include="str").columns

    for col in str_cols:
        df[col] = df[col].apply(lambda x: " ".join(x.split()) if isinstance(x, str) else x)

    return df