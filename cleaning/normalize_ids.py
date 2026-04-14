import pandas as pd


# Column name fragments that identify ID fields
ID_KEYWORDS = ("_id", "id_", "uuid", "guid", "key", "ref", "code")


def normalize_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Ensures ID columns are consistently typed as stripped strings with no mixed int/str values."""
    for col in df.columns:
        if any(kw in col.lower() for kw in ID_KEYWORDS):
            # Cast to string, strip whitespace, force lowercase for consistency
            df[col] = (
                df[col]
                .where(df[col].notna())          # keep NaN as NaN
                .apply(lambda v: str(int(v)) if isinstance(v, float) and not pd.isna(v) and v == int(v)
                       else str(v) if pd.notna(v) else v)
                .str.strip()
                .str.lower()
            )

    return df
