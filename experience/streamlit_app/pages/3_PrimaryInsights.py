import streamlit as st
import json
import pandas as pd
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
    st.subheader("Risk Arguments")
    args = []
    with open(args_path, "r") as f:
        for line in f:
            if line.strip(): args.append(json.loads(line))
            
    if args:
        df = pd.DataFrame(args)
        st.dataframe(df[['quote', 'five_c', 'proposed_delta']])
    else:
        st.write("No arguments extracted.")

impact_path = primary_dir / "impact_report.json"
if impact_path.exists():
    with open(impact_path, "r") as f:
        impact = json.load(f)
    st.subheader("Impact Report")
    st.json(impact)
