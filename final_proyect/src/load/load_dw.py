from pathlib import Path
import duckdb
import pandas as pd

SQL_DIR = Path("sql")
PROCESSED_DIR = Path("data/processed")
WAREHOUSE_DIR = Path("warehouse")
DB_PATH = WAREHOUSE_DIR / "dw.duckdb"

def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing parquet: {path}")
    return pd.read_parquet(path)

def create_schema(con: duckdb.DuckDBPyConnection):
    sql = (SQL_DIR / "create_tables.sql").read_text(encoding="utf-8")
    con.execute(sql)

def load_dimensions(con: duckdb.DuckDBPyConnection, alonso: pd.DataFrame, winners: pd.DataFrame):
    # Normalize columns (expected from your cleaning)
    # Alonso columns: year, grand_prix, team, race_number, grid_position, race_position, did_finish, event
    # Winners columns: year, grand_prix, date, circuit, continent, winner_name, team, laps, time
    # --- normalize strings to avoid hidden duplicates (spaces/case)
    alonso["grand_prix"] = alonso["grand_prix"].astype(str).str.strip().str.lower()
    alonso["team"] = alonso["team"].astype(str).str.strip()

    winners["grand_prix"] = winners["grand_prix"].astype(str).str.strip().str.lower()
    winners["team"] = winners["team"].astype(str).str.strip()
    winners["winner_name"] = winners["winner_name"].astype(str).str.strip()

    # ---- dim_season
    years = pd.concat([alonso[["year"]], winners[["year"]]], ignore_index=True).dropna()
    years = years.drop_duplicates().sort_values("year").reset_index(drop=True)
    years["season_id"] = range(1, len(years) + 1)
    con.register("tmp_years", years)
    con.execute("INSERT INTO dim_season SELECT season_id, year FROM tmp_years")

    # ---- dim_race (join key = year + grand_prix)
    races_a = alonso[["year", "grand_prix"]].dropna().drop_duplicates()
    races_w = winners[["year", "grand_prix", "date", "circuit", "continent"]].dropna(subset=["year", "grand_prix"]).drop_duplicates()

    # left merge to enrich with winners metadata if available
    races = races_a.merge(
        races_w,
        on=["year", "grand_prix"],
        how="outer"
    )

    # Ensure deterministic ids
    # Order & deduplicate: keep the "best" row (prefer the one with date/circuit info)
    races = races.sort_values(
        ["year", "grand_prix", "date", "circuit", "continent"],
        ascending=[True, True, False, False, False]
    )

# Drop duplicates on the natural key
    races = races.drop_duplicates(subset=["year", "grand_prix"], keep="first").reset_index(drop=True)

    races["race_id"] = range(1, len(races) + 1)

    con.register("tmp_races", races)
    con.execute("""
        INSERT INTO dim_race (race_id, year, grand_prix, date, circuit, continent)
        SELECT race_id, year, grand_prix, date, circuit, continent
        FROM tmp_races
    """)


    # ---- dim_driver
    drivers = pd.DataFrame({"driver_name": pd.concat([
        winners["winner_name"].dropna().astype(str).str.strip(),
        pd.Series(["Fernando Alonso"])
    ], ignore_index=True).drop_duplicates().sort_values().values})
    drivers["driver_id"] = range(1, len(drivers) + 1)

    con.register("tmp_drivers", drivers)
    con.execute("INSERT INTO dim_driver SELECT driver_id, driver_name FROM tmp_drivers")

    # ---- dim_team
    teams = pd.concat([
        alonso["team"].dropna().astype(str).str.strip(),
        winners["team"].dropna().astype(str).str.strip()
    ], ignore_index=True).drop_duplicates().sort_values().reset_index(drop=True)

    teams = pd.DataFrame({"team_name": teams})
    teams["team_id"] = range(1, len(teams) + 1)

    con.register("tmp_teams", teams)
    con.execute("INSERT INTO dim_team SELECT team_id, team_name FROM tmp_teams")

def load_facts(con: duckdb.DuckDBPyConnection, alonso: pd.DataFrame, winners: pd.DataFrame):
    # Register staging tables
    con.register("stg_alonso", alonso)
    con.register("stg_winners", winners)

    # ---- fact_race_winners
    con.execute("""
        INSERT INTO fact_race_winners
        SELECT
          row_number() OVER () AS fact_id,
          r.race_id,
          s.season_id,
          d.driver_id,
          t.team_id,
          CAST(w.laps AS INTEGER) AS laps,
          w.time
        FROM stg_winners w
        JOIN dim_race r
          ON r.year = w.year AND r.grand_prix = w.grand_prix
        JOIN dim_season s
          ON s.year = w.year
        JOIN dim_driver d
          ON d.driver_name = w.winner_name
        JOIN dim_team t
          ON t.team_name = w.team
    """)

    # ---- fact_alonso_race_results
    con.execute("""
        INSERT INTO fact_alonso_race_results
        SELECT
          row_number() OVER () AS fact_id,
          r.race_id,
          s.season_id,
          d.driver_id,
          t.team_id,
          CAST(a.race_number AS INTEGER) AS race_number,
          CAST(a.grid_position AS INTEGER) AS grid_position,
          CAST(a.race_position AS INTEGER) AS race_position,
          CAST(a.did_finish AS INTEGER) AS did_finish,
          a.event
        FROM stg_alonso a
        JOIN dim_race r
          ON r.year = a.year AND r.grand_prix = a.grand_prix
        JOIN dim_season s
          ON s.year = a.year
        JOIN dim_driver d
          ON d.driver_name = 'Fernando Alonso'
        JOIN dim_team t
          ON t.team_name = a.team
    """)

def main():
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    alonso = _read_parquet(PROCESSED_DIR / "alonso_clean.parquet")
    winners = _read_parquet(PROCESSED_DIR / "winners_clean.parquet")

    con = duckdb.connect(str(DB_PATH))

    create_schema(con)
    load_dimensions(con, alonso, winners)
    load_facts(con, alonso, winners)

    # quick sanity prints
    dim_counts = con.execute("""
      SELECT
        (SELECT COUNT(*) FROM dim_season) AS seasons,
        (SELECT COUNT(*) FROM dim_race)   AS races,
        (SELECT COUNT(*) FROM dim_driver) AS drivers,
        (SELECT COUNT(*) FROM dim_team)   AS teams,
        (SELECT COUNT(*) FROM fact_race_winners) AS winners_facts,
        (SELECT COUNT(*) FROM fact_alonso_race_results) AS alonso_facts
    """).fetchdf()

    print("DW created at:", DB_PATH)
    print(dim_counts.to_string(index=False))

    con.close()

if __name__ == "__main__":
    main()
