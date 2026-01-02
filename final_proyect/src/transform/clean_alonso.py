import pandas as pd

def clean_alonso(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # basic standardization
    df.columns = [c.strip().lower() for c in df.columns]
    df["grand_prix"] = df["grand_prix"].astype(str).str.strip()

    # numeric parsing
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["race_number"] = pd.to_numeric(df["race_number"], errors="coerce")
    df["grid_position"] = pd.to_numeric(df["grid_position"], errors="coerce")

    # race_position can be "ab" (abandoned). Convert to NaN.
    df["race_position_raw"] = df["race_position"]
    df["race_position"] = df["race_position"].replace({"ab": None})
    df["race_position"] = pd.to_numeric(df["race_position"], errors="coerce")

    # helper flag
    df["did_finish"] = df["race_position"].notna().astype(int)

    return df
