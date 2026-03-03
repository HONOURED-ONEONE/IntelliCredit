import streamlit as st
import json
import requests
from pathlib import Path
import yaml

st.set_page_config(page_title="Decision Engine", page_icon="⚖️")
st.title("4. Decision Engine")

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

decision_dir = job_dir / "decision_engine"
if not decision_dir.exists():
    st.info("Decision engine outputs not found yet.")
    st.stop()

cam_path = decision_dir / "cam.md"
if cam_path.exists():
    with open(cam_path, "r") as f:
        cam_md = f.read()
    st.markdown(cam_md)
    st.download_button("Download CAM", cam_md, file_name="cam.md")

col1, col2 = st.columns(2)
out_path = decision_dir / "decision_output.json"
if out_path.exists():
    with open(out_path, "r") as f:
        out_json = json.load(f)
    col1.subheader("Decision Output")
    col1.json(out_json)

breakdown_path = decision_dir / "score_breakdown.json"
if breakdown_path.exists():
    with open(breakdown_path, "r") as f:
        breakdown = json.load(f)
    col2.subheader("Score Breakdown")
    col2.json(breakdown)

api_url = st.session_state.get("api_url", "http://127.0.0.1:8000")
metrics_res = requests.get(f"{api_url}/jobs/{job_id}/metrics")
if metrics_res.status_code == 200:
    metrics = metrics_res.json()
    if "decision_narrative" in metrics:
        st.subheader("CAM LLM Metrics")
        st.json(metrics["decision_narrative"])
        
prov_res = requests.get(f"{api_url}/jobs/{job_id}/provenance")
if prov_res.status_code == 200:
    st.subheader("Provenance / Timings")
    st.json(prov_res.json().get("timing", {}))
