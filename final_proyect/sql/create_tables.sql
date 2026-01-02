-- Drop (for dev / reruns)
DROP TABLE IF EXISTS fact_alonso_race_results;
DROP TABLE IF EXISTS fact_race_winners;
DROP TABLE IF EXISTS dim_race;
DROP TABLE IF EXISTS dim_season;
DROP TABLE IF EXISTS dim_driver;
DROP TABLE IF EXISTS dim_team;

-- Dimensions
CREATE TABLE dim_season (
  season_id INTEGER PRIMARY KEY,
  year INTEGER NOT NULL UNIQUE
);

CREATE TABLE dim_race (
  race_id INTEGER PRIMARY KEY,
  year INTEGER NOT NULL,
  grand_prix VARCHAR NOT NULL,
  date DATE,
  circuit VARCHAR,
  continent VARCHAR,
  UNIQUE(year, grand_prix)
);

CREATE TABLE dim_driver (
  driver_id INTEGER PRIMARY KEY,
  driver_name VARCHAR NOT NULL UNIQUE
);

CREATE TABLE dim_team (
  team_id INTEGER PRIMARY KEY,
  team_name VARCHAR NOT NULL UNIQUE
);

-- Facts
CREATE TABLE fact_race_winners (
  fact_id BIGINT PRIMARY KEY,
  race_id INTEGER NOT NULL,
  season_id INTEGER NOT NULL,
  driver_id INTEGER NOT NULL,
  team_id INTEGER NOT NULL,
  laps INTEGER,
  time VARCHAR
);

CREATE TABLE fact_alonso_race_results (
  fact_id BIGINT PRIMARY KEY,
  race_id INTEGER NOT NULL,
  season_id INTEGER NOT NULL,
  driver_id INTEGER NOT NULL,
  team_id INTEGER NOT NULL,
  race_number INTEGER,
  grid_position INTEGER,
  race_position INTEGER,
  did_finish INTEGER,
  event VARCHAR
);
