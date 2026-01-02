# F1 Data Engineering Project – Architecture

## Goal
Build a reproducible batch data pipeline that ingests two related Formula 1 datasets, cleans and standardizes them, and loads a dimensional Data Warehouse (star schema) for analytics.

Datasets:
- **Fernando Alonso race results** (driver-specific, race-level)
- **F1 race winners 1950–2025** (global, winner-per-race)

## High-level architecture
The pipeline follows a classic analytics pattern:

**Raw (CSV) → Staging (clean Parquet) → Data Warehouse (DuckDB star schema) → Insights (SQL) → Delivery (BI / extracts)**

### Flow diagram (Mermaid)
```mermaid
flowchart LR
  A[Raw CSV files<br/>data/raw/] --> B[Extract<br/>pandas read_csv]
  B --> C[Transform/Clean<br/>standardize types, normalize strings, handle missing values]
  C --> D[Staging Layer<br/>data/processed/*.parquet]
  D --> E[Load to DuckDB Warehouse<br/>warehouse/dw.duckdb]
  E --> F[Analytics SQL<br/>sql/insights.sql]
  F --> G[Delivery<br/>Looker Studio / CSV exports / downstream consumers]


Data Warehouse model
A star schema is used to simplify analytical queries.

Dimensions
dim_season: one row per season (year).
dim_race: one row per race (year + grand_prix).
dim_driver: drivers (winners and Fernando Alonso).
dim_team: teams / constructors.

Facts
fact_race_winners
    Grain: one row per race winner.
fact_alonso_race_results
    Grain: one row per Fernando Alonso race result.

Star schema diagram
erDiagram
  DIM_SEASON ||--o{ FACT_RACE_WINNERS : season_id
  DIM_RACE   ||--o{ FACT_RACE_WINNERS : race_id
  DIM_DRIVER ||--o{ FACT_RACE_WINNERS : driver_id
  DIM_TEAM   ||--o{ FACT_RACE_WINNERS : team_id

  DIM_SEASON ||--o{ FACT_ALONSO_RACE_RESULTS : season_id
  DIM_RACE   ||--o{ FACT_ALONSO_RACE_RESULTS : race_id
  DIM_DRIVER ||--o{ FACT_ALONSO_RACE_RESULTS : driver_id
  DIM_TEAM   ||--o{ FACT_ALONSO_RACE_RESULTS : team_id

Data processing steps:
Extract: read raw CSV files from data/raw.
Transform: clean columns, normalize strings, handle missing values.
Processed layer: store cleaned data as Parquet files.
Load: populate dimension and fact tables in DuckDB.
Analytics: run SQL queries to generate insights.

Orchestration:
The pipeline can be executed daily using:
    python -m src.orchestration.run_pipeline
    python -m src.load.load_dw
Scheduling can be done with cron or a task scheduler.

Data quality:
Basic data quality checks are applied:
    datasets not empty
    required columns present
    valid year ranges
    deduplication of races by (year, grand_prix)