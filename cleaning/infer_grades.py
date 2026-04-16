import pandas as pd

def infer_grades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Infers 'grade' from 'score' using rules:

        0-13  → A
        14-27 → B
        28+   → C

    Rules:
    - If 'score' is missing → do nothing
    - If 'grade' already exists AND is not NA → do nothing
    - Otherwise, fill grade from score
    """

    df = df.copy()

    if "score" not in df.columns or "grade" not in df.columns:
        return df

    # Only compute where score exists AND grade is missing
    mask = df["score"].notna() & df["grade"].isna()

    scores = pd.to_numeric(df.loc[mask, "score"], errors="coerce")

    def map_grade(s):
        if pd.isna(s):
            return pd.NA
        if 0 <= s <= 13:
            return "A"
        elif 14 <= s <= 27:
            return "B"
        elif s >= 28:
            return "C"
        return pd.NA

    df.loc[mask, "grade"] = scores.apply(map_grade)

    return df