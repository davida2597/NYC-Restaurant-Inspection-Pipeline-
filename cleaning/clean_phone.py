import pandas as pd
import re

def clean_phone(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the 'phone' column by:
    - Removing all non-numeric characters
    - Keeping only digits
    - Ensuring exactly 10 digits remain
    - Otherwise setting value to pd.NA
    """

    df = df.copy()

    if "phone" not in df.columns:
        return df

    def extract_digits(value):
        if pd.isna(value):
            return pd.NA

        digits = re.sub(r"\D", "", str(value))

        if len(digits) == 10:
            return digits
        else:
            return pd.NA

    df["phone"] = df["phone"].apply(extract_digits)

    return df