from pathlib import Path
import pandas as pd

def extract_raw(raw_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    alonso_path = raw_dir / "fernandoalonso.csv"
    winners_path = raw_dir / "winners_f1_1950_2025_v2.csv"

    if not alonso_path.exists():
        raise FileNotFoundError(f"Missing file: {alonso_path}")
    if not winners_path.exists():
        raise FileNotFoundError(f"Missing file: {winners_path}")

    alonso = pd.read_csv(alonso_path)
    winners = pd.read_csv(winners_path)

    return alonso, winners
