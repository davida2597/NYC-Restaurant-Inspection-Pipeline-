import pandas as pd

def infer_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Infers missing inspection_date and grade_date values
    using the other column in the same row.

    Rules:
        - If inspection_date is NA → use grade_date
        - If grade_date is NA → use inspection_date
        - If both exist → keep original
        - If both missing → keep NA
    """

    df = df.copy()

    # Row-wise inference
    for idx, row in df.iterrows():

        insp = row.get("inspection_date")
        grade = row.get("grade_date")

        # If inspection_date missing, fill from grade_date
        if pd.isna(insp) and not pd.isna(grade):
            df.at[idx, "inspection_date"] = grade

        # If grade_date missing, fill from inspection_date
        elif pd.isna(grade) and not pd.isna(insp):
            df.at[idx, "grade_date"] = insp

    return df