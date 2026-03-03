import streamlit as st
import json
import pandas as pd
import requests
from pathlib import Path
import yaml

st.set_page_config(page_title="Primary Insights", page_icon="💡")
st.title("3. Primary Insights")

if "current_job_id" not in st.session_state:
    st.warning("Please start a job from the Ingest page first.")
    st.stop()

job_id = st.session_state["current_job_id"]
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
    st.info("Primary insights stage outputs not found yet.")
    st.stop()

args_path = primary_dir / "risk_arguments.jsonl"
if args_path.exists():
    st.subheader("Risk Arguments (LLM Extracted)")
    args = []
    with open(args_path, "r") as f:
        for line in f:
            if line.strip(): args.append(json.loads(line))
            
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
                    if line.strip(): links.append(json.loads(line))
            st.write(f"**Quote Links:** {len(links)}")
            if links:
                st.dataframe(pd.DataFrame(links))

api_url = st.session_state.get("api_url", "http://127.0.0.1:8000")

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
