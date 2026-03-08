# Architecture - MVP

IntelliCredit is composed of a FastAPI backend and a Streamlit frontend. It relies on a stateful, event-driven orchestration pipeline that builds deterministic decisions alongside reasoning-based LLM inferences.

## High-Level Flow
1. **Ingestor**: Reads CSV/PDFs from Mock, Uploads, or Databricks. Optionally uses Vision LLM (GPT-4o) for OCR. Emits `facts.jsonl` and `signals.json`.
2. **Research**: Performs web queries using Perplexity, Tavily, SerpAPI, or Mock JSONL datasets. Identifies adverse media. Emits `research_findings.jsonl`.
3. **Primary Insights**: Consumes the inputs and builds structured risk arguments via Claude 3.7. Emits `risk_arguments.jsonl`.
4. **Decision Engine**: Calculates a deterministic risk score based entirely on quantitative inputs and findings from previous stages. Automatically renders a `cam.md` file and converts it to `.docx` and `.pdf`.

## Observability & Guardrails
- **Validation**: Strict JSON Schema adherence.
- **Guardrails**: Halts or REFERS applications if inputs fail validation or plausibility checks.
- **Resilience**: Configurable retry policies powered by `tenacity`.
- **Size Limits**: Hard-capped upload limits managed by Starlette middleware to mitigate abuse.
