from pathlib import Path
import duckdb

DB_PATH = Path("warehouse/dw.duckdb")
SQL_PATH = Path("sql/insights.sql")

from src.logging_setup import setup_logging
import logging

    
INSIGHT_DESCRIPTIONS = [
    "Total number of Formula 1 races Fernando Alonso has participated in, "
    "and how many of those races ended in a DNF (Did Not Finish).",

    "Average finishing position of Fernando Alonso per season, "
    "considering only races that he finished.",

    "Top 10 Formula 1 drivers by total number of race wins.",

    "Total number of Formula 1 race wins per team.",

    "Total number of Formula 1 race wins achieved by Fernando Alonso.",

    "Distribution of Fernando Alonso's podium finishes "
    "(1st, 2nd and 3rd places).",

    "Number of Formula 1 races held per continent.",

    "Average difference between Fernando Alonso's starting grid position "
    "and his finishing position. Negative values mean positions gained."
]


def main():
    setup_logging()
    logger = logging.getLogger("analysis.run_insights")
    logger.info("Running insights")
    con = duckdb.connect(str(DB_PATH))
    sql = SQL_PATH.read_text(encoding="utf-8")

    queries = [q.strip() for q in sql.split(";") if q.strip()]

    for i, q in enumerate(queries):
        print("\n" + "=" * 70)
        print(f"INSIGHT {i + 1}")
        print(INSIGHT_DESCRIPTIONS[i])
        print("-" * 70)

        df = con.execute(q).fetchdf()
        print(df.to_string(index=False))
        
    logger.info("Insights execution finished")
    con.close()


if __name__ == "__main__":
    main()
