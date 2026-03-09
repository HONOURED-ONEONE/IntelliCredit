# experience/streamlit_app/pages/3_PrimaryInsights.py
import streamlit as st
import json
import pandas as pd
import requests
import sys
import time
from pathlib import Path

# Add the parent directory to sys.path to allow importing core_utils
project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from experience.streamlit_app.core_utils import get_api_url, get_provider_mode, fetch_artifact

st.set_page_config(page_title="Primary Insights", page_icon="💡")
st.title("3. Primary Insights")

api_url = get_api_url()
provider_mode = get_provider_mode()

st.subheader("Officer Notes")
notes = st.text_area(
    "Paste officer notes here. Quotes are recommended, as extraction is quote-first.",
    '',
    help="These notes will be analyzed to extract risk arguments aligned to the 5Cs."
)

# Offer a way to start a new job carrying the notes.
with st.form("primary_run_form"):
    run_with_notes = st.form_submit_button("Start New Job with Notes")
    if run_with_notes:
        payload = {
            "notes": notes,
            "source_mode": provider_mode,
            "enable_live_llm": st.session_state.get("enable_live_llm", False),
            "enable_live_search": st.session_state.get("enable_live_search", False),
            "enable_live_databricks": st.session_state.get("enable_live_databricks", False),
            "source": "streamlit"
        }
        try:
            res = requests.post(f"{api_url}/jobs", json=payload)
            res.raise_for_status()
            job_id = res.json()["job_id"]
            st.session_state["current_job_id"] = job_id
            st.success(f"Job started! ID: {job_id}")
        except Exception as e:
            st.error(f"Failed to start job with notes: {e}")

if "current_job_id" not in st.session_state:
    st.info("No active job found. Start a job from Ingest or the button above.")
    st.stop()

job_id = st.session_state["current_job_id"]

# Optional: wait a bit for primary stage to finish when job is already running
with st.expander("Monitor current job"):
    status_ph = st.empty()
    for _ in range(20):
        try:
            s_res = requests.get(f"{api_url}/jobs/{job_id}")
            if s_res.status_code == 200:
                stage = s_res.json()["stage"]
                status_ph.info(f"**Current Stage:** {stage}")
                if stage == "completed":
                    status_ph.success("**Job Completed!**")
                    break
        except requests.exceptions.RequestException:
            pass
        time.sleep(1)

args_content = fetch_artifact(api_url, job_id, "primary/risk_arguments.jsonl")
if args_content:
    st.subheader("Risk Arguments (LLM Extracted)")
    args = []
    for line in args_content.splitlines():
        if line.strip():
            try:
                args.append(json.loads(line))
            except Exception:
                pass
    if args:
        df = pd.DataFrame(args)
        disp_cols = ['quote', 'five_c', 'proposed_delta', 'note_missing_quote']
        if 'contradicts' in df.columns:
            disp_cols.append('contradicts')
        st.dataframe(df[[c for c in disp_cols if c in df.columns]])
    else:
        st.write("No arguments extracted.")
else:
    st.info("Primary insights artifacts not found yet.")

impact_data = fetch_artifact(api_url, job_id, "primary/impact_report.json")
if impact_data:
    st.subheader("Impact Report")
    st.json(impact_data)

contradictions_data = fetch_artifact(api_url, job_id, "primary/contradictions.json")
quote_links_content = fetch_artifact(api_url, job_id, "primary/quote_links.jsonl")

if contradictions_data or quote_links_content:
    with st.expander("Guardrails & Heuristics (Sidecars)"):
        if contradictions_data:
            st.write(f"**Contradictions Detected:** {len(contradictions_data.get('pairs', []))}")
            st.json(contradictions_data)
        if quote_links_content:
            links = []
            for line in quote_links_content.splitlines():
                if line.strip():
                    try:
                        links.append(json.loads(line))
                    except Exception:
                        pass
            st.write(f"**Quote Links:** {len(links)}")
            if links:
                st.dataframe(pd.DataFrame(links))

# Validation/Metrics
val_res = requests.get(f"{api_url}/jobs/{job_id}/validation?stage=primary")
if val_res.status_code == 200:
    st.subheader("Validation Summary")
    rep = val_res.json()
    st.json(rep.get("summary", {}))

metrics_res = requests.get(f"{api_url}/jobs/{job_id}/metrics")
if metrics_res.status_code == 200:
    metrics = metrics_res.json()
    if "primary_reasoning" in metrics:
        st.subheader("LLM Metrics")
        st.json(metrics["primary_reasoning"])
    if "primary_repair" in metrics:
        st.warning("Schema Repair Triggered!")
        st.json(metrics["primary_repair"])