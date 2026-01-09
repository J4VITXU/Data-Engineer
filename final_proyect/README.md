# Formula 1 Data Engineering Project

This project implements a complete Data Engineering pipeline using Python and DuckDB,
from raw CSV ingestion to analytical insights.

## Project Structure

- `src/orchestration/run_pipeline.py`
  - Extracts raw CSV data
  - Cleans and validates datasets
  - Writes staging parquet files

- `src/load/load_dw.py`
  - Loads processed data into a DuckDB Data Warehouse
  - Creates dimension and fact tables

- `src/load/dw_checks.py`
  - Performs Data Warehouse quality checks
  - Validates foreign keys, grain and expected row counts

- `src/analysis/run_insights.py`
  - Executes analytical SQL queries over the Data Warehouse
  - Prints insights to the console

## Execution Order

The scripts must be executed in the following order:

```bash
# 1. Extract & Transform (staging)
python -m src.orchestration.run_pipeline

# 2. Load Data Warehouse
python -m src.load.load_dw

# 3. Validate Data Warehouse
python -m src.transform.dw_checks

# 4. Run analytical insights
python -m src.analysis.run_insights
