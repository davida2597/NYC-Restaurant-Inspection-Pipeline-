import pandas as pd


def normalize_caps(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercases all string columns, then title-cases columns that likely contain proper names."""
    name_keywords = ("name", "title", "city", "state", "country", "category",
                     "label", "dba", "boro", "borough", "street", "cuisine")

    # Support both pandas 2 (object) and pandas 3 (str dtype)
    string_cols = (
        set(df.select_dtypes(include="object").columns)
        | set(df.select_dtypes(include="str").columns)
    )

    for col in df.columns:
        if col not in string_cols:
            continue
        if any(kw in col.lower() for kw in name_keywords):
            df[col] = df[col].str.strip().str.title()
        else:
            df[col] = df[col].str.lower()

    return df
