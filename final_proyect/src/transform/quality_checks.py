import pandas as pd

class DataQualityError(Exception):
    pass

def check_not_empty(df: pd.DataFrame, name: str):
    if df.empty:
        raise DataQualityError(f"{name} is empty")

def check_required_columns(df: pd.DataFrame, name: str, required: list[str]):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise DataQualityError(f"{name} missing columns: {missing}")

def check_year_valid(df: pd.DataFrame, name: str, col: str = "year"):
    if df[col].isna().any():
        raise DataQualityError(f"{name} has NULL years")
    if (df[col] < 1900).any() or (df[col] > 2100).any():
        raise DataQualityError(f"{name} has out-of-range years")

def check_grand_prix_not_null(df: pd.DataFrame, name: str):
    if df["grand_prix"].isna().any() or (df["grand_prix"].astype(str).str.strip() == "").any():
        raise DataQualityError(f"{name} has NULL/blank grand_prix")

def run_all_checks(alonso: pd.DataFrame, winners: pd.DataFrame):
    check_not_empty(alonso, "alonso")
    check_not_empty(winners, "winners")

    check_required_columns(alonso, "alonso", ["year", "grand_prix", "race_number", "team", "grid_position", "race_position"])
    check_required_columns(winners, "winners", ["year", "grand_prix", "winner_name", "team", "date"])

    check_year_valid(alonso, "alonso")
    check_year_valid(winners, "winners")
    check_grand_prix_not_null(alonso, "alonso")
    check_grand_prix_not_null(winners, "winners")
