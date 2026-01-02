import pandas as pd

def clean_winners(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.columns = [c.strip().lower() for c in df.columns]
    df["grand_prix"] = df["grand_prix"].astype(str).str.strip()
    df["winner_name"] = df["winner_name"].astype(str).str.strip()
    df["team"] = df["team"].astype(str).str.strip()

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["laps"] = pd.to_numeric(df["laps"], errors="coerce")

    # date parsing (may contain invalid rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    return df
