# IntelliCredit MVP

Welcome to the IntelliCredit MVP. This system automates deterministic and LLM-assisted credit assessments via a structured orchestration pipeline.

## Deployment Stack
- **UI (Frontend):** Hosted on Streamlit Community Cloud. The UI is completely API-driven and does not rely on local filesystem access for fetching artifacts.
- **Backend (API):** Hosted on Railway via FastAPI. Exposes the endpoints to run jobs, fetch statuses, and securely serve validation/evidence artifacts.
- **Docker Compose:** Remains primarily for local-dev.

## Run Profiles

1. **Local Uploads (Default for Production)**: Upload PDFs and CSVs manually via the Streamlit UI.
2. **Local Mock (offline)**: Runs completely offline using mock CSVs and data for local testing. No API keys required.
3. **Databricks Files / Tables**: Integrates directly with Databricks Volumes/DBFS and Unity Catalog. **Requires** `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and `DATABRICKS_HTTP_PATH`. Disabled by default and will fail if enabled without credentials.
   - *Databricks Files Mode (Live)*: Hydrates PDFs directly from DBFS.
   - *UC Schema Mapping*: Arbitrary Databricks Unity Catalog tables are gracefully mapped to canonical schemas.

## Search & Citations

- **Improved Disambiguation**: Research now employs Jaccard token similarity for fuzzy matching canonical names and aliases. Entity profiles are generated in `research/entities/profile.json` with an `entity_confidence` score (0.6-1.0 range).
- **Perplexity Integration**: When using the Perplexity provider (via `PPLX_API_KEY`), the results are now automatically parsed into structured citations (up to 5 per query) and include heuristics for source quality rating (+20 for `.gov` or `reuters`). Citations are deduplicated based on canonical URLs.
- **Metasearch Ensemble**: Combine results from Perplexity, Tavily, and SerpAPI via Rank Reciprocal Fusion (Strategy 2) for robust adverse media research. Requires `SERPAPI_API_KEY` along with `TAVILY_API_KEY` and `PPLX_API_KEY`.
- **IndianKanoon Stub**: Provides a stub for synthetic legal citations. Enable via `search.legal_sources.indiankanoon.enabled: true`. Adds up to 2 legal citation links per entity and tracks `legal_hits` in the entity profile.

## Governance & Evidence

- **Evidence Page Images**: Set `governance.evidence.store_page_images: true` in `config/base.yaml` to export low-res JPEG snapshots of processed PDF pages into `evidence_pack/docs/pages/`. Hashes for these images are appended to `evidence_manifest.json`.
- **Analytical Heuristics Summary**: A new `spike_reversal_summary.json` is generated in the evidence pack, capturing robust outlier detection results and circular trading risk scores.

## Primary Guardrails

- **Contradiction Detection**: Semantic heuristics detect if two extracted arguments from officer notes contradict each other (opposing polarity or significant delta differences on the same 5C dimension). Results are stored in `primary/contradictions.json`.
- **Freshness Decay**: If `visit_date` is supplied, the `proposed_delta` of arguments degrades using a 90-day half-life curve. Modifiers are logged to `impact_report.json` and `weights.json` leaving the core contract unmutated.
- **Quote Linking**: LLM-extracted quotes from officer notes are fuzzily linked back to the ingested PDF pages via the `documents.jsonl` artifact, creating a direct lineage in `primary/quote_links.jsonl` which is appended to the evidence pack.

## Decision Policy & Pricing

- **Deterministic Policy Matrix**: Decisions can now be referred based on risk indicators like low entity confidence, high circular trading risk, or detected legal hits via a configurable `policy_matrix` in `base.yaml`.
- **YAML-Driven Pricing**: The Decision Engine derives its scoring model, threshold values, and pricing curves directly from `config/base.yaml` via the `decision:` block, rather than hardcoded logic.
- **Circular Trading Risk**: Analytical heuristics now detect "spikes" and "reversals" across GST and Bank series. A `circular_trading_risk` score is computed and surfaced in `signals.json`, influencing the policy matrix.
- **Backward Compatible**: The provided default configuration perfectly reproduces the legacy deterministic logic.
- **Inline Policy Gates**: Any `CRITICAL` issues in upstream validation reports automatically trigger a `REFER` action inside the engine and append the block reason directly to the CAM narrative drivers.
### OCR & Cleanup (Optional)
Extraction hardening includes optional graceful OCR fallback and light image cleanup.
To use OCR, install the optional dependencies (`pytesseract`, `opencv-python`) and system dependency `tesseract`.

Feature flags in `config/base.yaml`:
```yaml
features:
  ocr:
    enabled: true
  cleanup:
    enabled: true
```
If disabled or dependencies are missing, the pipeline gracefully falls back to basic `pdfplumber` extraction with zero changes.

## API & Centralized Uploads

- **POST /jobs/{id}/uploads**: Centralized endpoint for uploading GST, Bank CSVs, and multiple PDFs. Streamlit no longer writes directly to the job directory.
- **GET /jobs/{id}/inputs**: Manifest of all uploaded input files with SHA256 checksums for provenance.
- **Cross-Stage Validation Aggregator**: A new `/jobs/{id}/validation/aggregate` endpoint serves a rolled-up summary of all per-stage schema and business logic validations.

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
