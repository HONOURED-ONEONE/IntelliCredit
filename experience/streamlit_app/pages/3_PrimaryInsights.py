# experience/streamlit_app/pages/3_PrimaryInsights.py
import streamlit as st
import json
import pandas as pd
import requests
from pathlib import Path
import yaml
import time

st.set_page_config(page_title="Primary Insights", page_icon="💡")
st.title("3. Primary Insights")

api_url = st.session_state.get("api_url", "http://127.0.0.1:8000")
provider_mode = st.session_state.get("provider_mode", "mock")

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

project_root = Path(__file__).resolve().parent.parent.parent.parent
config_path = project_root / "config" / "base.yaml"
output_root = "outputs/jobs"
if config_path.exists():
    with open(config_path, "r") as cf:
        conf = yaml.safe_load(cf)
        output_root = conf.get("paths", {}).get("output_root", "outputs/jobs")

job_dir = project_root / output_root / job_id
primary_dir = job_dir / "primary"
if not primary_dir.exists():
    st.info("Primary insights artifacts not found yet.")
    st.stop()

args_path = primary_dir / "risk_arguments.jsonl"
if args_path.exists():
    st.subheader("Risk Arguments (LLM Extracted)")
    args = []
    with open(args_path, "r") as f:
        for line in f:
            if line.strip():
                args.append(json.loads(line))
    if args:
        df = pd.DataFrame(args)
        disp_cols = ['quote', 'five_c', 'proposed_delta', 'note_missing_quote']
        if 'contradicts' in df.columns:
            disp_cols.append('contradicts')
        st.dataframe(df[[c for c in disp_cols if c in df.columns]])
    else:
        st.write("No arguments extracted.")

impact_path = primary_dir / "impact_report.json"
if impact_path.exists():
    with open(impact_path, "r") as f:
        impact = json.load(f)
    st.subheader("Impact Report")
    st.json(impact)

contradictions_path = primary_dir / "contradictions.json"
quote_links_path = primary_dir / "quote_links.jsonl"
if contradictions_path.exists() or quote_links_path.exists():
    with st.expander("Guardrails & Heuristics (Sidecars)"):
        if contradictions_path.exists():
            with open(contradictions_path, "r") as f:
                contra = json.load(f)
            st.write(f"**Contradictions Detected:** {len(contra.get('pairs', []))}")
            st.json(contra)
        if quote_links_path.exists():
            links = []
            with open(quote_links_path, "r") as f:
                for line in f:
                    if line.strip():
                        links.append(json.loads(line))
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