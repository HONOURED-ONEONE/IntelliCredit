import streamlit as st
import json
import pandas as pd
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
        st.dataframe(df[['entity', 'claim', 'stance', 'citation_url']])
    else:
        st.write("No findings extracted.")
