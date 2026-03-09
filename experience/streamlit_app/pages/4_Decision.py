import streamlit as st
import json
import requests
import sys
from pathlib import Path

# Add the parent directory to sys.path to allow importing core_utils
project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from experience.streamlit_app.core_utils import get_api_url, fetch_artifact

st.set_page_config(page_title="Decision Engine", page_icon="⚖️")
st.title("4. Decision Engine")

if "current_job_id" not in st.session_state:
    st.warning("Please start a job from the Ingest page first.")
    st.stop()

job_id = st.session_state["current_job_id"]
api_url = get_api_url()

cam_md = fetch_artifact(api_url, job_id, "decision_engine/cam.md")
if cam_md:
    st.markdown(cam_md)
    st.download_button("Download CAM", cam_md, file_name="cam.md")
    
    st.info("To download CAM (DOCX/PDF), please use the API directly or check project directory if running locally.")
else:
    st.info("Decision engine outputs not found yet.")
    st.stop()

col1, col2 = st.columns(2)
out_json = fetch_artifact(api_url, job_id, "decision_engine/decision_output.json")
if out_json:
    col1.subheader("Decision Output")
    col1.json(out_json)

breakdown = fetch_artifact(api_url, job_id, "decision_engine/score_breakdown.json")
if breakdown:
    col2.subheader("Score Breakdown")
    col2.json(breakdown)

val_res = requests.get(f"{api_url}/jobs/{job_id}/validation?stage=decision")
if val_res.status_code == 200:
    st.subheader("Validation Summary")
    rep = val_res.json()
    st.json(rep.get("summary", {}))

    if out_json:
        if out_json.get("decision") == "REFER":
            st.error("Decision outcome is REFER. Please review reasons below:")
            st.write(out_json.get("drivers", []))

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
