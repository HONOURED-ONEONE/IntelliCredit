import streamlit as st
import json
import pandas as pd
import requests
import sys
from pathlib import Path

# Add the parent directory to sys.path to allow importing core_utils
project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from experience.streamlit_app.core_utils import get_api_url, fetch_artifact

st.set_page_config(page_title="Research", page_icon="🔍")
st.title("2. Secondary Research")

if "current_job_id" not in st.session_state:
    st.warning("Please start a job from the Ingest page first.")
    st.stop()

job_id = st.session_state["current_job_id"]
api_url = get_api_url()

summary_content = fetch_artifact(api_url, job_id, "research/research_summary.md")
if summary_content:
    st.markdown(summary_content)
else:
    st.info("Research stage outputs not found or not completed yet.")

findings_content = fetch_artifact(api_url, job_id, "research/research_findings.jsonl")
if findings_content:
    st.subheader("Research Findings List")
    findings = []
    for line in findings_content.splitlines():
        if line.strip():
            try:
                findings.append(json.loads(line))
            except Exception:
                pass
    
    if findings:
        df = pd.DataFrame(findings)
        df['citation_url'] = df['citations'].apply(lambda c: c[0]['url'] if c else "")
        df['source_quality'] = df['citations'].apply(lambda c: c[0].get('source_quality', 0) if c else 0)
        st.dataframe(df[['entity', 'claim', 'stance', 'citation_url', 'source_quality']])

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
