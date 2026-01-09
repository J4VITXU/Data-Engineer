from pathlib import Path
import logging

from src.logging_setup import setup_logging
from src.extract.extract_raw import extract_raw
from src.transform.clean_alonso import clean_alonso
from src.transform.clean_winners import clean_winners
from src.transform.quality_checks import run_all_checks

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/processed")

def main():
    setup_logging()
    logger = logging.getLogger("orchestration.run_pipeline")

    logger.info("Starting pipeline: extract -> transform -> quality checks -> parquet")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Extracting raw data...")
    alonso_raw, winners_raw = extract_raw(RAW_DIR)

    logger.info("Cleaning datasets...")
    alonso = clean_alonso(alonso_raw)
    winners = clean_winners(winners_raw)

    logger.info("Running data quality checks...")
    run_all_checks(alonso, winners)

    logger.info("Writing staging parquet files...")
    alonso.to_parquet(OUT_DIR / "alonso_clean.parquet", index=False)
    winners.to_parquet(OUT_DIR / "winners_clean.parquet", index=False)

    logger.info("DONE: staging parquet files created in data/processed/")

if __name__ == "__main__":
    main()
