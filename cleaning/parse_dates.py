import pandas as pd


# Matched as suffixes/exact names to avoid false positives
# (e.g. 'phone', 'action', 'location', 'latitude' all contain short keywords)
DATE_SUFFIXES = ("_date", "_at", "_time", "_on", "_timestamp")
DATE_EXACT = {"date", "timestamp", "created", "updated", "datetime"}


def _is_date_column(col: str) -> bool:
    col_lower = col.lower()
    return (
        any(col_lower.endswith(suf) for suf in DATE_SUFFIXES)
        or col_lower in DATE_EXACT
    )


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parses date/datetime strings into proper datetime objects for likely date columns."""
    # Support both pandas 2 (object dtype) and pandas 3 (str dtype)
    string_cols = (
        set(df.select_dtypes(include="object").columns)
        | set(df.select_dtypes(include="str").columns)
    )

    for col in df.columns:
        if col not in string_cols or not _is_date_column(col):
            continue
        try:
            parsed = pd.to_datetime(df[col], errors="coerce")
            # Only commit if at least half the non-null values parsed successfully
            if parsed.notna().sum() >= df[col].notna().sum() * 0.5:
                df[col] = parsed
        except Exception:
            pass  # Leave untouched if parsing fails entirely

    return df
