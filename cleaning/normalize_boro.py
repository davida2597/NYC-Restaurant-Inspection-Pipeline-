import pandas as pd

def normalize_boro(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace all string '0' values in the 'boro' column with pd.NA.

    Args:
        df (pd.DataFrame): Input dataframe

    Returns:
        pd.DataFrame: Updated dataframe
    """

    df = df.copy()

    if "boro" in df.columns:
        df["boro"] = df["boro"].replace("0", pd.NA)

    return df