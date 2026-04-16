import pandas as pd

def normalize_caps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Capitalize the first letter of each word in specified columns.

    Handles apostrophes correctly:
        "SPENCER'S" -> "Spencer's"
        "O'BRIEN"   -> "O'Brien"

    Args:
        df (pd.DataFrame): Input dataframe
        
    Returns:
        pd.DataFrame: Updated dataframe
    """

    df = df.copy()

    # Decide which columns to clean
    columns = ['dba', 'boro', 'street', 'inspection_type']

    def fix_word(word: str) -> str:
        parts = word.split("'")
        parts = [p.capitalize() if p else p for p in parts]

        # keep possessive 's lowercase
        if len(parts) > 1 and parts[-1].lower() == "s":
            parts[-1] = "s"

        return "'".join(parts)

    def fix_cell(x):
        if not isinstance(x, str):
            return x

        tokens = x.split()
        tokens = [fix_word(t.lower()) for t in tokens]

        return " ".join(tokens)

    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(fix_cell)

    return df