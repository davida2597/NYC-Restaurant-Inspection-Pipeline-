import pandas as pd

def parse_dates(df: pd.DataFrame) -> pd.DataFrame:

    date_cols = ['inspection_date', 'grade_date', 'record_date']

    df = df.copy()

    for col in date_cols:
        if col not in df.columns:
            continue

        def try_parse(x):
            if pd.isna(x):
                return x
            try:
                return pd.to_datetime(x)
            except Exception:
                return x

        df[col] = df[col].apply(try_parse)

    return df