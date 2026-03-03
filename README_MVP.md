# IntelliCredit MVP

Welcome to the IntelliCredit MVP. This system automates deterministic and LLM-assisted credit assessments via a structured orchestration pipeline.

## Run Profiles

1. **Local Mock (offline)**: Runs completely offline using mock CSVs and data. No API keys required.
2. **Local Uploads**: Upload PDFs and CSVs manually via the Streamlit UI.
3. **Databricks Files / Tables**: Integrates directly with Databricks Volumes/DBFS and Unity Catalog. Requires `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and `DATABRICKS_HTTP_PATH`.

## How to Run (Local Docker Compose)

1. Copy `.env.example` to `.env`. Fill out any API keys if you plan to use live LLMs or live Search. You do not need any keys for the offline mock mode.
2. Build and start the services:
   `docker-compose up --build`
3. Access the UI at [http://localhost:8501](http://localhost:8501).

## Architecture

The pipeline consists of 4 stages: Ingestor -> Research -> Primary Insights -> Decision Engine. Every stage outputs strictly typed and validated JSON schemas (`contracts`) and builds up an evidence pack. If critical constraints fail, the pipeline falls back gracefully to a REFER status and still generates the required CAM documentation.

## Health
- **API Live check:** `GET http://localhost:8000/health/live`
- **API Ready check:** `GET http://localhost:8000/health/ready` (Validates write access and loaded API keys).

## Artifacts
All outputs are stored in `outputs/jobs/{job_id}/`. A visual tree can be queried via `GET /results/{job_id}?tree=true`.
