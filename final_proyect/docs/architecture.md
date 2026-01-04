# Project Architecture

## Overview

This project implements a modular Data Engineering architecture that separates
data ingestion, transformation, storage and analytics.

The main goal is to build a reliable and auditable Data Warehouse for
Formula 1 historical data, enabling analytical queries and insights.

---

## Architectural Layers

### 1. Orchestration
**Location:** `src/orchestration`

Responsible for coordinating the pipeline execution without performing
business logic.

- `run_pipeline.py`
  - Orchestrates extract and transform steps
  - Writes staging parquet files

---

### 2. Extract
**Location:** `src/extract`

Reads raw CSV files from disk and returns pandas DataFrames.

- No data cleaning
- No business logic

This ensures raw data traceability.

---

### 3. Transform
**Location:** `src/transform`

Responsible for data cleaning, normalization and validation.

- `clean_alonso.py`
- `clean_winners.py`
- `dw_checks.py` (Data Warehouse validation)

Transformations are applied before loading data into the Data Warehouse.

---

### 4. Load
**Location:** `src/load`

Responsible for loading processed data into DuckDB.

- `load_dw.py`
  - Creates dimension and fact tables
  - Enforces consistent grain and relationships

The Data Warehouse is rebuilt from scratch on each execution.

---

### 5. Analysis
**Location:** `src/analysis`

Consumes the Data Warehouse to produce analytical insights.

- `run_insights.py`
  - Executes predefined SQL queries
  - Prints results to the console

No transformations or data corrections are allowed at this stage.

---

## Data Warehouse Design

### Dimension Tables

- `dim_season`: Formula 1 seasons (year)
- `dim_race`: Unique races (year, date, circuit)
- `dim_driver`: Drivers
- `dim_team`: Teams

### Fact Tables

#### `fact_race_winners`
- Grain: one row per winner per race
- Allows historical shared wins

#### `fact_alonso_race_results`
- Grain: one row per Fernando Alonso participation
- Includes finishing position, grid position and race outcome

---

## Data Quality Strategy

Data quality is enforced at two levels:

1. **Pre-load checks**
   - Validation of cleaned datasets

2. **Post-load checks**
   - Foreign key validation
   - Grain enforcement
   - Expected row counts

The pipeline stops if critical checks fail.

---

## Logging

All executable scripts use centralized logging configuration.
Logs are written both to the console and to `logs/pipeline.log`.

---

## Execution Flow

```text
run_pipeline → load_dw → dw_checks → run_insights
