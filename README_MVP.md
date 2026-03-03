# IntelliCredit MVP

Welcome to the IntelliCredit MVP. This system automates deterministic and LLM-assisted credit assessments via a structured orchestration pipeline.

## Run Profiles

1. **Local Mock (offline)**: Runs completely offline using mock CSVs and data. No API keys required.
2. **Local Uploads**: Upload PDFs and CSVs manually via the Streamlit UI.
3. **Databricks Files / Tables**: Integrates directly with Databricks Volumes/DBFS and Unity Catalog. Requires `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and `DATABRICKS_HTTP_PATH`. *Note: The Live Databricks files mode now supports full DBFS file hydration into the local inputs directory before extraction.*

## Search & Citations

- **Perplexity Integration**: When using the Perplexity provider (via `PPLX_API_KEY`), the results are now automatically parsed into structured citations (up to 5 per query) and include heuristics for source quality rating (+20 for `.gov` or `reuters`).

## Governance & Evidence

- **Evidence Page Images**: Set `governance.evidence.store_page_images: true` in `config/base.yaml` to export low-res JPEG snapshots of processed PDF pages into `evidence_pack/docs/pages/`. Hashes for these images are appended to `evidence_manifest.json`.

## Primary Guardrails

- **Contradiction Detection**: Semantic heuristics detect if two extracted arguments from officer notes contradict each other (opposing polarity or significant delta differences on the same 5C dimension). Results are stored in `primary/contradictions.json`.
- **Freshness Decay**: If `visit_date` is supplied, the `proposed_delta` of arguments degrades using a 90-day half-life curve. Modifiers are logged to `impact_report.json` and `weights.json` leaving the core contract unmutated.
- **Quote Linking**: LLM-extracted quotes from officer notes are fuzzily linked back to the ingested PDF pages via the `documents.jsonl` artifact, creating a direct lineage in `primary/quote_links.jsonl` which is appended to the evidence pack.

## Decision Policy & Pricing

- **YAML-Driven Pricing**: The Decision Engine derives its scoring model, threshold values, and pricing curves directly from `config/base.yaml` via the `decision:` block, rather than hardcoded logic.
- **Backward Compatible**: The provided default configuration perfectly reproduces the legacy deterministic logic.
- **Inline Policy Gates**: Any `CRITICAL` issues in upstream validation reports automatically trigger a `REFER` action inside the engine and append the block reason directly to the CAM narrative drivers.

## Ops & Governance (P3)

- **Cross-Stage Validation Aggregator**: A new `/jobs/{id}/validation/aggregate` endpoint serves a rolled-up summary of all per-stage schema and business logic validations.
- **Token & Cost Telemetry**: Granular tracking for prompt/completion tokens and provider API calls. Cost limits can be calculated dynamically by mapping usage to configurable `billing` rates in `config/base.yaml` (default rate is 0.0 to prevent surprise estimates).
- **Prometheus Metrics Export**: Easily monitor active pipelines. Enable `metrics.prometheus.enabled: true` in `config/base.yaml` to expose the `/metrics` endpoint with latency histograms, stage validation counts, token usage, and cost projections. (Requires `prometheus_client`). Note: All features are backward compatible and strictly opt-in.

## How to Run (Local Docker Compose)

1. Copy `.env.example` to `.env`. Fill out any API keys if you plan to use live LLMs or live Search. You do not need any keys for the offline mock mode.
2. Build and start the services:
   `docker-compose up --build`
3. Access the UI at [http://localhost:8501](http://localhost:8501).

## Architecture

The pipeline consists of 4 stages: Ingestor -> Research -> Primary Insights -> Decision Engine. Every stage outputs strictly typed and validated JSON schemas (`contracts`) and builds up an evidence pack. If critical constraints fail, the pipeline falls back gracefully to a REFER status and still generates the required CAM documentation.

## Health
- **API Live check:** `GET http://localhost:8000/health/live`
- **API Ready check:** `GET http://localhost:8000/health/ready` (Validates write access, and clearly exposes granular readiness statuses for Live LLMs, Search APIs, and Databricks endpoints without failing overall offline viability).

## Artifacts
All outputs are stored in `outputs/jobs/{job_id}/`. A visual tree can be queried via `GET /results/{job_id}?tree=true`.
