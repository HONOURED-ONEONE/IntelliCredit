import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="IntelliCredit • Overview",
    page_icon="🏦",
    layout="wide"
)

# ---- hero ----
st.title("IntelliCredit — Automated Credit Analysis (MVP)")
st.markdown(
    """
**Deterministic + LLM‑assisted** credit assessments with full **validation, provenance,
and evidence packaging** — designed for Indian financial workflows.
"""
)

# ---- mini-presentation: Architecture ----
left, right = st.columns([1, 1], gap="large")

with left:
    st.subheader("System Architecture (MVP)")

    st.code(
        r"""
+-------------------+        +-------------------+        +-------------------+
|   Streamlit App   |  --->  |   FastAPI (API)   |  --->  |  Orchestration    |
|  (Front-end UX)   |        |  Health/Results   |        |  Job Runner,      |
|    Pages 1..5     |        |  Uploads, Metrics |        |  Stages, Idempot. |
+-------------------+        +-------------------+        +-------------------+
                                                                |
                                                                v
    +------------------- Pipeline (Stages) -------------------------------+
    | 1) Ingestor  | 2) Research  | 3) Primary Insights | 4) Decision    |
    |  - CSV/PDF   |  - Web/Ensm. |  - Quote-first LLM  |  - Deterministic|
    |  - OCR/Vision|  - Citations |  - Guardrails       |  - Policy Matrix|
    +---------------------------------------------------------------------+
                                                                |
                                                                v
    +----------------- Governance & Evidence ---------------------------+
    | Validation (per stage & aggregate), Provenance, Metrics, Evidence |
    +-------------------------------------------------------------------+

Storage: outputs/jobs/{job_id}/... (artifacts, reports, CAM, evidence_pack)
        """,
        language="text",
    )

with right:
    st.subheader("Key Building Blocks")
    st.markdown(
        """
- **Streamlit App**: Operator UX for starting jobs, uploading files, browsing artifacts.
- **FastAPI**: Health checks, uploads, job status, validation/evidence endpoints.
- **Orchestration**: Async job runner with per‑stage idempotency & schema tagging.
- **Stages**:
  - **Ingestor**: 
    - Reads GST/Bank CSVs; parses PDFs (pdfplumber), optional OCR (Tesseract),
      optional Vision LLM extraction. Emits `facts.jsonl`, `signals.json`.
  - **Research**:
    - Live/metasearch (Perplexity/Tavily/SerpAPI Mock) with **RRF fusion**;
      outputs `research_findings.jsonl` + sidecars.
  - **Primary Insights**:
    - Quote‑first extraction via Anthropics/OpenAI (if enabled) with repair and
      guardrails (contradictions, quote‑links).
  - **Decision**:
    - Deterministic scoring + policy matrix; renders **CAM** (`.md`, `.docx`, `.pdf`).
- **Governance**:
  - Stage validations, aggregate validation, metrics, provenance, evidence pack.
"""
    )

st.markdown("---")

# ---- functional view ----
st.subheader("Functional Modules")

c1, c2, c3 = st.columns(3, gap="large")

with c1:
    st.markdown("### 1) Ingestor")
    st.write(
        "- Inputs: GST returns, Bank statements, PDFs\n"
        "- Extracts totals & facts; optional OCR & Vision\n"
        "- **Artifacts**: `ingestor/facts.jsonl`, `ingestor/signals.json`"
    )

with c2:
    st.markdown("### 2) Research")
    st.write(
        "- Providers: Perplexity, Tavily, SerpAPI, Mock\n"
        "- **Ensemble fusion (RRF)** & risk escalation\n"
        "- **Artifacts**: `research/research_findings.jsonl`, `secondary_research/*`"
    )

with c3:
    st.markdown("### 3) Primary Insights")
    st.write(
        "- Officer notes → quote‑first arguments\n"
        "- Freshness decay, contradictions, quote‑links\n"
        "- **Artifacts**: `primary/risk_arguments.jsonl`, `impact_report.json`"
    )

c4, c5 = st.columns(2, gap="large")

with c4:
    st.markdown("### 4) Decision")
    st.write(
        "- Deterministic score + policy matrix REFERS on risk\n"
        "- **Artifacts**: `decision_engine/decision_output.json`, `cam.md/.docx/.pdf`"
    )

with c5:
    st.markdown("### 5) Governance")
    st.write(
        "- Per‑stage & aggregate validation; provenance & metrics\n"
        "- Evidence pack (citations, snippets, page images)\n"
        "- **Artifacts**: `*_validation_report.json`, `validation_aggregate.json`, `evidence_pack/*`"
    )

st.markdown("---")

# ---- how to run ----
st.subheader("How It Runs")
st.markdown(
    """
1. **Local Mock**: No keys required. Uses bundled CSVs/PDFs and mock research.
2. **Local Uploads**: Upload your PDFs/CSVs via **Ingest**; pipeline runs end‑to‑end.
3. **Databricks** *(optional)*: Pull files/tables from DBFS/UC when live integration is enabled.
4. **Live Providers** *(optional)*: LLMs (OpenAI/Anthropic), Search (Perplexity/Tavily/SerpAPI).
"""
)

# ---- quick navigation ----
st.subheader("Navigate")
cols = st.columns(3)
cols[0].page_link("pages/1_Ingest.py", label="Ingest", icon="📥")
cols[1].page_link("pages/2_Research.py", label="Research", icon="🔎")
cols[2].page_link("pages/3_PrimaryInsights.py", label="Primary Insights", icon="💡")

cols2 = st.columns(3)
cols2[0].page_link("pages/4_Decision.py", label="Decision", icon="⚖️")
cols2[1].page_link("pages/5_Validation_Provenance.py", label="Validation & Provenance", icon="🛡️")

st.markdown("---")