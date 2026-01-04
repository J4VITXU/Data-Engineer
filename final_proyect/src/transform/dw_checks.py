import duckdb
from pathlib import Path

from src.logging_setup import setup_logging
import logging

DB_PATH = Path("warehouse/dw.duckdb")

def main():
    setup_logging()
    logger = logging.getLogger("quality.dw_checks")
    logger.info("Starting DW checks")
    con = duckdb.connect(str(DB_PATH))

    counts = con.execute("""
      SELECT
        (SELECT COUNT(*) FROM dim_season) AS seasons,
        (SELECT COUNT(*) FROM dim_race)   AS races,
        (SELECT COUNT(*) FROM dim_driver) AS drivers,
        (SELECT COUNT(*) FROM dim_team)   AS teams,
        (SELECT COUNT(*) FROM fact_race_winners) AS winners_facts,
        (SELECT COUNT(*) FROM fact_alonso_race_results) AS alonso_facts
    """).fetchone()

    seasons, races, drivers, teams, winners_facts, alonso_facts = counts
    print(f"seasons={seasons} races={races} drivers={drivers} teams={teams} "
          f"winners_facts={winners_facts} alonso_facts={alonso_facts}")

    # checks “esperados” (ajusta si tu dataset cambia)
    assert races > 0 and seasons > 0 and drivers > 0 and teams > 0, "Empty dimensions? Load failed."
    assert winners_facts >= races, "Winners facts should be >= races (shared wins possible)."
    # Si tu CSV dice 384 participaciones:
    assert alonso_facts == 384, f"Expected 384 Alonso participations, got {alonso_facts}"

    # 2) FK checks (los tuyos + completos)
    fk_winners_race = con.execute("""
      SELECT COUNT(*)
      FROM fact_race_winners f
      LEFT JOIN dim_race d ON f.race_id = d.race_id
      WHERE d.race_id IS NULL
    """).fetchone()[0]

    fk_winners_driver = con.execute("""
      SELECT COUNT(*)
      FROM fact_race_winners f
      LEFT JOIN dim_driver d ON f.driver_id = d.driver_id
      WHERE d.driver_id IS NULL
    """).fetchone()[0]

    fk_winners_team = con.execute("""
      SELECT COUNT(*)
      FROM fact_race_winners f
      LEFT JOIN dim_team d ON f.team_id = d.team_id
      WHERE d.team_id IS NULL
    """).fetchone()[0]

    fk_alonso_race = con.execute("""
      SELECT COUNT(*)
      FROM fact_alonso_race_results f
      LEFT JOIN dim_race d ON f.race_id = d.race_id
      WHERE d.race_id IS NULL
    """).fetchone()[0]

    fk_alonso_team = con.execute("""
      SELECT COUNT(*)
      FROM fact_alonso_race_results f
      LEFT JOIN dim_team d ON f.team_id = d.team_id
      WHERE d.team_id IS NULL
    """).fetchone()[0]

    fk_alonso_driver = con.execute("""
      SELECT COUNT(*)
      FROM fact_alonso_race_results f
      LEFT JOIN dim_driver d ON f.driver_id = d.driver_id
      WHERE d.driver_id IS NULL
    """).fetchone()[0]

    assert fk_winners_race == 0, f"Missing dim_race for winners facts: {fk_winners_race}"
    assert fk_winners_driver == 0, f"Missing dim_driver for winners facts: {fk_winners_driver}"
    assert fk_winners_team == 0, f"Missing dim_team for winners facts: {fk_winners_team}"
    assert fk_alonso_race == 0, f"Missing dim_race for alonso facts: {fk_alonso_race}"
    assert fk_alonso_team == 0, f"Missing dim_team for alonso facts: {fk_alonso_team}"
    assert fk_alonso_driver == 0, f"Missing dim_driver for alonso facts: {fk_alonso_driver}"

    # 3a) dim_race clave única (year,date,circuit) ya tiene UNIQUE, pero lo comprobamos
    dup_races = con.execute("""
      SELECT COUNT(*)
      FROM (
        SELECT year, date, circuit, COUNT(*) n
        FROM dim_race
        GROUP BY year, date, circuit
        HAVING COUNT(*) > 1
      )
    """).fetchone()[0]
    assert dup_races == 0, f"Duplicate races in dim_race (year,date,circuit): {dup_races}"

    # 3b) fact_alonso: una fila por race_id (Alonso participa una vez por carrera)
    dup_alonso_race = con.execute("""
      SELECT COUNT(*)
      FROM (
        SELECT race_id, COUNT(*) n
        FROM fact_alonso_race_results
        GROUP BY race_id
        HAVING COUNT(*) > 1
      )
    """).fetchone()[0]
    assert dup_alonso_race == 0, f"Duplicate Alonso rows per race_id: {dup_alonso_race}"

    # 3c) fact_winners: puede haber >1 por carrera, pero nunca debería ser 0 por carrera
    races_without_winner = con.execute("""
      SELECT COUNT(*)
      FROM dim_race r
      LEFT JOIN fact_race_winners f ON f.race_id = r.race_id
      WHERE f.race_id IS NULL
    """).fetchone()[0]
    assert races_without_winner == 0, f"Races with no winner fact: {races_without_winner}"

    print("OK: DW checks passed.")
    con.close()

if __name__ == "__main__":
    main()
