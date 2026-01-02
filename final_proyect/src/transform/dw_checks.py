import duckdb
from pathlib import Path

DB_PATH = Path("warehouse/dw.duckdb")

def main():
    con = duckdb.connect(str(DB_PATH))

    # FK checks
    fk1 = con.execute("""
      SELECT COUNT(*) AS missing
      FROM fact_race_winners f
      LEFT JOIN dim_race d ON f.race_id = d.race_id
      WHERE d.race_id IS NULL
    """).fetchone()[0]

    fk2 = con.execute("""
      SELECT COUNT(*) AS missing
      FROM fact_alonso_race_results f
      LEFT JOIN dim_team d ON f.team_id = d.team_id
      WHERE d.team_id IS NULL
    """).fetchone()[0]

    assert fk1 == 0, f"Missing dim_race for winners facts: {fk1}"
    assert fk2 == 0, f"Missing dim_team for alonso facts: {fk2}"

    print("OK: DW checks passed.")
    con.close()

if __name__ == "__main__":
    main()
