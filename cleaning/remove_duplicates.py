import pandas as pd


# Column(s) to use as the uniqueness key. If empty, all columns are used.
UNIQUE_ON: list[str] = []


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Drops exact duplicate rows within the batch, keeping the first occurrence."""
    before = len(df)

    subset = [c for c in UNIQUE_ON if c in df.columns] or None
    df = df.drop_duplicates(subset=subset, keep="first")

    dropped = before - len(df)
    if dropped:
        print(f"  remove_duplicates: removed {dropped} duplicate rows")

    return df
