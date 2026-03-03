import streamlit as st
import json
import pandas as pd
import requests
from pathlib import Path
import yaml

st.set_page_config(page_title="Research", page_icon="🔍")
st.title("2. Secondary Research")

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

research_dir = job_dir / "research"
if not research_dir.exists():
    st.info("Research stage outputs not found yet.")
    st.stop()

summary_path = research_dir / "research_summary.md"
if summary_path.exists():
    with open(summary_path, "r") as f:
        st.markdown(f.read())

findings_path = research_dir / "research_findings.jsonl"
if findings_path.exists():
    st.subheader("Research Findings List")
    findings = []
    with open(findings_path, "r") as f:
        for line in f:
            if line.strip(): findings.append(json.loads(line))
    
    if findings:
        df = pd.DataFrame(findings)
        df['citation_url'] = df['citations'].apply(lambda c: c[0]['url'] if c else "")
        df['source_quality'] = df['citations'].apply(lambda c: c[0].get('source_quality', 0) if c else 0)
        st.dataframe(df[['entity', 'claim', 'stance', 'citation_url', 'source_quality']])

api_url = st.session_state.get("api_url", "http://127.0.0.1:8000")

val_res = requests.get(f"{api_url}/jobs/{job_id}/validation?stage=research")
if val_res.status_code == 200:
    st.subheader("Validation Summary")
    rep = val_res.json()
    st.json(rep.get("summary", {}))

metrics_res = requests.get(f"{api_url}/jobs/{job_id}/metrics")
if metrics_res.status_code == 200:
    metrics = metrics_res.json()
    if "research" in metrics:
        st.subheader("Metrics")
        st.json(metrics["research"])
