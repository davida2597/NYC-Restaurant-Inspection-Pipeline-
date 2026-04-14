import pandas as pd


# Map column name fragments → target dtype.
NUMERIC_KEYWORDS = ("amount", "value", "price", "count", "total", "qty", "quantity",
                    "score", "rate", "latitude", "longitude", "age",
                    "size", "weight", "height", "length", "width")

BOOL_KEYWORDS = ("is_", "has_", "flag", "active", "enabled", "deleted")


def validate_types(df: pd.DataFrame) -> pd.DataFrame:
    """Coerces numeric columns to float and boolean columns to bool; drops rows with invalid values."""
    before = len(df)
    invalid_mask = pd.Series(False, index=df.index)

    # Support both pandas 2 (object) and pandas 3 (str dtype)
    string_cols = (
        set(df.select_dtypes(include="object").columns)
        | set(df.select_dtypes(include="str").columns)
    )

    for col in df.columns:
        col_lower = col.lower()

        if any(kw in col_lower for kw in NUMERIC_KEYWORDS):
            coerced = pd.to_numeric(df[col], errors="coerce")
            newly_invalid = coerced.isna() & df[col].notna()
            if newly_invalid.any():
                print(f"  validate_types: {newly_invalid.sum()} unparseable value(s) in '{col}'")
            invalid_mask |= newly_invalid
            df[col] = coerced

        elif any(kw in col_lower for kw in BOOL_KEYWORDS):
            if col in string_cols:
                df[col] = df[col].map(
                    {"true": True, "false": False, "1": True, "0": False,
                     "yes": True, "no": False, True: True, False: False}
                )

    df = df[~invalid_mask]
    dropped = before - len(df)
    if dropped:
        print(f"  validate_types: removed {dropped} rows with invalid type values")

    return df
