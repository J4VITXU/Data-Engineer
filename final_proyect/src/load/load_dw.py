from pathlib import Path
import duckdb
import pandas as pd

SQL_DIR = Path("sql")
PROCESSED_DIR = Path("data/processed")
WAREHOUSE_DIR = Path("warehouse")
DB_PATH = WAREHOUSE_DIR / "dw.duckdb"


# Helpers
def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing parquet: {path}")
    return pd.read_parquet(path)


def _norm_text(s: pd.Series) -> pd.Series:
    # Normaliza texto de forma "safe" para joins
    return (
        s.astype(str)
        .str.replace("\u00a0", " ", regex=False)
        .str.strip()
    )


def create_schema(con: duckdb.DuckDBPyConnection):
    sql_path = SQL_DIR / "create_tables.sql"
    if not sql_path.exists():
        raise FileNotFoundError(f"Missing SQL schema file: {sql_path}")
    con.execute(sql_path.read_text(encoding="utf-8"))


# Dimensions
def load_dimensions(
    con: duckdb.DuckDBPyConnection,
    alonso: pd.DataFrame,
    winners: pd.DataFrame,
) -> pd.DataFrame:
    alonso = alonso.copy()
    winners = winners.copy()

    # ---- types + normalization
    # Alonso
    alonso["year"] = pd.to_numeric(alonso["year"], errors="coerce").astype("Int64")
    alonso["team"] = _norm_text(alonso.get("team", pd.Series(dtype="object")))
    alonso["race_number"] = pd.to_numeric(alonso.get("race_number", pd.Series(dtype="object")), errors="coerce")
    for col in ["grid_position", "race_position", "did_finish", "event"]:
        if col not in alonso.columns:
            alonso[col] = pd.NA

    # Winners
    winners["year"] = pd.to_numeric(winners["year"], errors="coerce").astype("Int64")
    winners["date"] = pd.to_datetime(winners["date"], errors="coerce").dt.date
    winners["grand_prix"] = _norm_text(winners.get("grand_prix", pd.Series(dtype="object")))
    winners["circuit"] = _norm_text(winners.get("circuit", pd.Series(dtype="object")))
    winners["continent"] = _norm_text(winners.get("continent", pd.Series(dtype="object")))
    winners["team"] = _norm_text(winners.get("team", pd.Series(dtype="object")))
    winners["winner_name"] = _norm_text(winners.get("winner_name", pd.Series(dtype="object")))

    # ---------------- dim_season
    years = pd.concat([alonso[["year"]], winners[["year"]]], ignore_index=True).dropna()
    years = years.drop_duplicates().sort_values("year").reset_index(drop=True)
    years["season_id"] = range(1, len(years) + 1)

    con.register("tmp_years", years)
    con.execute("INSERT INTO dim_season (season_id, year) SELECT season_id, year FROM tmp_years")

    # ---------------- dim_race
    # 1 fila = 1 carrera real => clave única (year, date, circuit)
    w_cal = winners[["year", "date", "circuit", "grand_prix", "continent"]].dropna(subset=["year", "date", "circuit"]).copy()

    w_cal = (
        w_cal.sort_values(["year", "date", "circuit", "grand_prix"])
        .drop_duplicates(subset=["year", "date", "circuit"], keep="first")
        .reset_index(drop=True)
    )

    # race_number = round dentro del año (derivado por fecha/circuito)
    w_cal = w_cal.sort_values(["year", "date", "circuit"]).reset_index(drop=True)
    w_cal["race_number"] = w_cal.groupby("year").cumcount() + 1

    dim_race_df = w_cal.rename(columns={"grand_prix": "grand_prix"}).copy()
    dim_race_df["race_id"] = range(1, len(dim_race_df) + 1)

    con.register("tmp_races", dim_race_df)
    con.execute("""
        INSERT INTO dim_race (race_id, year, race_number, grand_prix, date, circuit, continent)
        SELECT race_id, year, race_number, grand_prix, date, circuit, continent
        FROM tmp_races
    """)

    # ---------------- dim_driver
    drivers = pd.concat(
        [winners["winner_name"].dropna(), pd.Series(["Fernando Alonso"])],
        ignore_index=True
    ).drop_duplicates().sort_values().reset_index(drop=True)

    dim_driver_df = pd.DataFrame({"driver_name": drivers})
    dim_driver_df["driver_id"] = range(1, len(dim_driver_df) + 1)

    con.register("tmp_drivers", dim_driver_df)
    con.execute("INSERT INTO dim_driver (driver_id, driver_name) SELECT driver_id, driver_name FROM tmp_drivers")

    # ---------------- dim_team
    teams = pd.concat([alonso["team"].dropna(), winners["team"].dropna()], ignore_index=True)
    teams = teams.drop_duplicates().sort_values().reset_index(drop=True)

    dim_team_df = pd.DataFrame({"team_name": teams})
    dim_team_df["team_id"] = range(1, len(dim_team_df) + 1)

    con.register("tmp_teams", dim_team_df)
    con.execute("INSERT INTO dim_team (team_id, team_name) SELECT team_id, team_name FROM tmp_teams")

    return alonso


# -------------------------
# Facts
# -------------------------
def load_facts(
    con: duckdb.DuckDBPyConnection,
    alonso: pd.DataFrame,
    winners: pd.DataFrame,
):
    winners = winners.copy()
    winners["year"] = pd.to_numeric(winners["year"], errors="coerce").astype("Int64")
    winners["date"] = pd.to_datetime(winners["date"], errors="coerce").dt.date
    winners["circuit"] = _norm_text(winners.get("circuit", pd.Series(dtype="object")))
    winners["team"] = _norm_text(winners.get("team", pd.Series(dtype="object")))
    winners["winner_name"] = _norm_text(winners.get("winner_name", pd.Series(dtype="object")))

    alonso = alonso.copy()
    alonso["year"] = pd.to_numeric(alonso["year"], errors="coerce").astype("Int64")
    alonso["team"] = _norm_text(alonso.get("team", pd.Series(dtype="object")))
    alonso["race_number"] = pd.to_numeric(alonso.get("race_number", pd.Series(dtype="object")), errors="coerce")

    alonso = alonso.sort_values(["year", "race_number"], na_position="last").reset_index(drop=True)
    alonso["season_round"] = alonso.groupby("year").cumcount() + 1

    con.register("stg_winners", winners)
    con.register("stg_alonso", alonso)

    # ---- fact_race_winners
    con.execute("""
        INSERT INTO fact_race_winners
        SELECT
          row_number() OVER (ORDER BY w.year, w.date, w.circuit, w.winner_name) AS fact_id,
          r.race_id,
          s.season_id,
          d.driver_id,
          t.team_id,
          CAST(w.laps AS INTEGER) AS laps,
          w.time
        FROM stg_winners w
        JOIN dim_race r
          ON r.year = w.year AND r.date = w.date AND r.circuit = w.circuit
        JOIN dim_season s
          ON s.year = w.year
        JOIN dim_driver d
          ON d.driver_name = w.winner_name
        JOIN dim_team t
          ON t.team_name = w.team
        WHERE w.year IS NOT NULL AND w.date IS NOT NULL AND w.circuit IS NOT NULL
    """)

    # ---- fact_alonso_race_results
    con.execute("""
        INSERT INTO fact_alonso_race_results
        SELECT
          row_number() OVER (ORDER BY a.year, a.season_round) AS fact_id,
          r.race_id,
          s.season_id,
          d.driver_id,
          t.team_id,
          CAST(a.season_round AS INTEGER) AS race_number,
          CAST(a.grid_position AS INTEGER) AS grid_position,
          CAST(a.race_position AS INTEGER) AS race_position,
          CAST(a.did_finish AS INTEGER) AS did_finish,
          a.event
        FROM stg_alonso a
        JOIN dim_race r
          ON r.year = a.year AND r.race_number = a.season_round
        JOIN dim_season s
          ON s.year = a.year
        JOIN dim_driver d
          ON d.driver_name = 'Fernando Alonso'
        JOIN dim_team t
          ON t.team_name = a.team
        WHERE a.year IS NOT NULL
    """)


def main():
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    alonso = _read_parquet(PROCESSED_DIR / "alonso_clean.parquet")
    winners = _read_parquet(PROCESSED_DIR / "winners_clean.parquet")

    con = duckdb.connect(str(DB_PATH))

    create_schema(con)

    alonso_clean = load_dimensions(con, alonso, winners)
    load_facts(con, alonso_clean, winners)

    # Checks finales
    counts = con.execute("""
      SELECT
        (SELECT COUNT(*) FROM dim_season) AS seasons,
        (SELECT COUNT(*) FROM dim_race)   AS races,
        (SELECT COUNT(*) FROM dim_driver) AS drivers,
        (SELECT COUNT(*) FROM dim_team)   AS teams,
        (SELECT COUNT(*) FROM fact_race_winners) AS winners_facts,
        (SELECT COUNT(*) FROM fact_alonso_race_results) AS alonso_facts
    """).fetchdf()


    print("DW created at:", DB_PATH)
    print(counts.to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
