# Copilot Instructions for Citation Bias Explorer

## Project Overview
- **Purpose:** Analyze citation disparities across countries/institutions using OpenAlex data, PySpark ETL, and Streamlit dashboard.
- **Pipeline:**
  1. **Ingest**: Fetch works from OpenAlex API (`src/ingest/fetch_works.py`).
  2. **ETL**: Flatten and curate data with PySpark (`src/etl/spark_jobs.py`, `src/etl/schema.py`).
  3. **Metrics**: Compute normalized citation metrics (`src/metrics/`).
  4. **App**: Interactive dashboard in Streamlit (`src/app/`).

## Key Workflows
- **Data Ingestion:**
  - Run: `python3 -m src.ingest.fetch_works`
  - Configurable via `.env` (see `.env.example` for required variables).
  - Output: Raw JSONL in `data/raw/works.jsonl`.
- **ETL Processing:**
  - Use PySpark jobs in `src/etl/` to curate and flatten data.
  - Output: Parquet/CSV in `data/curated/`.
- **Metrics Calculation:**
  - Scripts in `src/metrics/` compute citation bias indices.
- **Dashboard Launch:**
  - Run Streamlit app from `src/app/` to visualize results.

## Architecture & Patterns
- **Directory Structure:**
  - `src/ingest/`: API client, data fetch logic.
  - `src/etl/`: Spark schema definitions, ETL jobs.
  - `src/metrics/`: Metric computation scripts.
  - `src/app/`: Streamlit dashboard code.
- **Data Flow:**
  - Ingest → ETL → Metrics → App.
  - Data moves from `data/raw/` (JSONL) → `data/curated/` (Parquet/CSV).
- **Environment Variables:**
  - All API/config settings via `.env` (copy from `.env.example`).
- **Testing:**
  - Unit tests in `tests/`.
  - CI via GitHub Actions (`.github/workflows/ci.yml`).

## Conventions & Tips
- **PySpark:**
  - Use explicit schema (`src/etl/schema.py`).
  - ETL jobs expect input in `data/raw/works.jsonl`.
- **Data Files:**
  - `data/raw/` and `data/curated/` are ignored by git.
- **Extensibility:**
  - Add new metrics in `src/metrics/`.
  - Extend dashboard in `src/app/`.
- **Docker:**
  - Containerization supported via `Dockerfile`.

## Example: Adding a New Metric
1. Create script in `src/metrics/`.
2. Read curated data from `data/curated/`.
3. Output results to new file or dashboard.

---
For questions or unclear patterns, review `README.md` or ask for clarification.
