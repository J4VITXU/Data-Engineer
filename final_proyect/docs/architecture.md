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

Transformations are applied before loading data into the Data Warehouse.

---

### 4. Load
**Location:** `src/load`

Responsible for loading processed data into DuckDB.

- `load_dw.py`
  - Creates dimension and fact tables
  - Enforces consistent grain and relationships
- `dw_checks.py` (Data Warehouse validation)

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

run_pipeline → load_dw → dw_checks → run_insights

## Scalability (x10 / x100 / x1000 / x10^6)

This project runs locally with **pandas** (transform) + **DuckDB** (warehouse). It works well for small/medium data, but scaling requires changes.

- **x10 rows**
  - Still OK on a laptop.
  - Improve by: using **parquet**, avoiding copies in pandas, and pushing some joins/aggregations to DuckDB SQL.

- **x100 rows**
  - pandas may struggle with RAM and runtime.
  - Improve by: partition parquet by `year`, and avoid full rebuilds (process only new data).

- **x1000 rows**
  - Single-machine pipeline becomes a bottleneck.
  - Proposal: move transforms to **Spark/Dask** and store raw/staging in **object storage** (S3/GCS). Use a cloud warehouse.

- **x10^6 rows**
  - Needs cloud-native architecture.
  - Proposal: orchestration (Airflow/Prefect), distributed compute (Spark), and a scalable warehouse (BigQuery/Snowflake/Redshift).

Key idea: to scale, replace **full refresh** with **incremental loads** (only new races/seasons each day).

---

## Cloud Costs

Example provider: **Google Cloud (GCP)**.

**Cloud version architecture**
- Storage: **GCS** (raw + staging parquet)
- Compute/orchestration: **Cloud Scheduler + Cloud Run** (or Airflow for bigger scale)
- Warehouse: **BigQuery**
- Monitoring: **Cloud Logging + Cloud Monitoring**

**How costs grow**
- Main cost drivers: **data storage** + **compute time** + **warehouse queries scanned**.
- Rough scaling (relative):
  - x10: Low → Medium
  - x100: Medium → High
  - x1000: High → Very High
  - x10^6: Very High (needs strict cost control)

**Cost control tips**
- Partition/cluster tables by `year`
- Use parquet + partitions (scan less data)
- Incremental loads (avoid rebuilding everything)
- Avoid `SELECT *` in analytics

---

## Data Consumers

Who can use the data?
- **Students/analysts**: run SQL on the warehouse
- **BI dashboards**: charts and reports (e.g., Looker Studio)
- **Developers**: export tables/views to build apps or notebooks

How to deliver it?
- Locally: DuckDB + SQL queries
- Cloud: BigQuery connected to Looker Studio

---

## How AI Can Help

AI can help with:
- **Data quality**: detect anomalies (missing races, weird spikes in DNFs)
- **Normalization**: suggest mapping for team/driver name variants
- **Monitoring**: summarize logs and highlight failures
- **Insights**: generate plain-English explanations for non-technical users

---

## Privacy

This project uses **public Formula 1 historical data**, so privacy risk is low.

Good practices anyway:
- keep data read-only, avoid publishing unnecessary raw files
- if future versions include user data, apply GDPR-style controls (access control, encryption, minimization)

