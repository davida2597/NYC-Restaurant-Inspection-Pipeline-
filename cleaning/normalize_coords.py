import pandas as pd

def normalize_coords(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces latitude/longitude values of 0 with pd.NA.
    If either latitude or longitude is invalid (0),
    also sets 'location' to pd.NA for that row.
    """

    df = df.copy()

    if "latitude" not in df.columns or "longitude" not in df.columns:
        return df

    # Convert to numeric safely
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # Identify invalid rows
    invalid_geo = (df["latitude"] == 0) | (df["longitude"] == 0)

    # Set lat/lon to NA where invalid
    df.loc[df["latitude"] == 0, "latitude"] = pd.NA
    df.loc[df["longitude"] == 0, "longitude"] = pd.NA

    # If either is invalid, also nullify location
    df.loc[invalid_geo, "location"] = pd.NA

    return df