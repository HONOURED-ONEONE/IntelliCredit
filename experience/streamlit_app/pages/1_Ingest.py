import streamlit as st
import requests
import time
import json
import os
import yaml
from pathlib import Path

st.set_page_config(page_title="Ingest Job", page_icon="📥")

st.title("Ingest Data")

api_url = st.session_state.get("api_url", "http://127.0.0.1:8000")

source_text = st.text_input("Source", "sample_source_data")
start_button = st.button("Start Job")

if start_button:
    if not source_text:
        st.error("Please enter a source.")
    else:
        try:
            response = requests.post(f"{api_url}/jobs", json={"source": source_text})
            response.raise_for_status()
            job_data = response.json()
            job_id = job_data["job_id"]
            
            st.success(f"Job started! ID: {job_id}")
            st.session_state["current_job_id"] = job_id
        except Exception as e:
            st.error(f"Failed to start job: {e}")

if "current_job_id" in st.session_state:
    job_id = st.session_state["current_job_id"]
    st.markdown(f"### Monitoring Job: `{job_id}`")
    
    status_placeholder = st.empty()
    artifacts_placeholder = st.empty()
    
    # Poll for status
    for _ in range(10):
        try:
            status_res = requests.get(f"{api_url}/jobs/{job_id}")
            if status_res.status_code == 200:
                status_data = status_res.json()
                stage = status_data["stage"]
                status_placeholder.info(f"**Current Stage:** {stage}")
                if stage == "completed":
                    status_placeholder.success(f"**Job Completed!**")
                    break
            else:
                status_placeholder.warning("Waiting for job status...")
        except requests.exceptions.RequestException:
            status_placeholder.error("Cannot connect to API.")
            break
        time.sleep(1)
        
    # Fetch artifacts
    try:
        results_res = requests.get(f"{api_url}/results/{job_id}")
        if results_res.status_code == 200:
            files = results_res.json().get("files", [])
            if files:
                artifacts_placeholder.markdown("### Artifacts")
                for f in files:
                    st.write(f"- {f['name']} ({f['size_bytes']} bytes)")
                    
                # Load config to find path
                # Use absolute path resolving from this file upwards assuming standard structure
                # Since this is in pages/1_Ingest.py we need .parent.parent.parent.parent
                project_root = Path(__file__).resolve().parent.parent.parent.parent
                config_path = project_root / "config" / "base.yaml"
                output_root = "outputs/jobs"
                if config_path.exists():
                    with open(config_path, "r") as cf:
                        conf = yaml.safe_load(cf)
                        output_root = conf.get("paths", {}).get("output_root", "outputs/jobs")
                        
                job_dir = project_root / output_root / job_id
                
                st.markdown("#### Downloads")
                col1, col2 = st.columns(2)
                
                status_file = job_dir / "status.json"
                if status_file.exists():
                    with open(status_file, "rb") as sf:
                        col1.download_button("Download status.json", sf, file_name="status.json", mime="application/json")
                        
                prov_file = job_dir / "provenance.json"
                if prov_file.exists():
                    with open(prov_file, "rb") as pf:
                        col2.download_button("Download provenance.json", pf, file_name="provenance.json", mime="application/json")
                        
    except Exception as e:
        st.error(f"Failed to fetch artifacts: {e}")
