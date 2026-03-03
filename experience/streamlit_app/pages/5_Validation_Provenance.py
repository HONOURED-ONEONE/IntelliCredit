import streamlit as st
import json
import requests
from pathlib import Path
import pandas as pd

st.set_page_config(page_title="Validation & Provenance", page_icon="🛡️")
st.title("5. Validation & Provenance")

if "current_job_id" not in st.session_state:
    st.warning("Please start a job from the Ingest page first.")
    st.stop()

job_id = st.session_state["current_job_id"]
api_url = st.session_state.get("api_url", "http://127.0.0.1:8000")

tab1, tab2, tab3, tab4 = st.tabs(["Validation", "Provenance", "Metrics", "Evidence"])

# 1. Validation Tab
with tab1:
    st.header("Stage Validation Reports")
    stages = ["ingestor", "research", "primary", "decision"]
    for stage in stages:
        st.subheader(f"{stage.title()} Validation")
        res = requests.get(f"{api_url}/jobs/{job_id}/validation?stage={stage}")
        if res.status_code == 200:
            rep = res.json()
            st.write(f"**Schema OK:** {rep.get('schema_ok')}")
            st.json(rep.get("summary", {}))
            
            issues = rep.get("issues", [])
            if issues:
                df = pd.DataFrame(issues)
                
                def color_severity(val):
                    color = 'red' if val == 'CRITICAL' else ('orange' if val == 'WARN' else 'green')
                    return f'color: {color}'
                
                st.dataframe(df.style.map(color_severity, subset=['severity']))
            else:
                st.success("No issues found.")
        else:
            st.info(f"No validation report for {stage} yet.")

# 2. Provenance Tab
with tab2:
    st.header("Provenance Record")
    res = requests.get(f"{api_url}/jobs/{job_id}/provenance")
    if res.status_code == 200:
        prov = res.json()
        st.json(prov)
    else:
        st.info("Provenance data not available.")

# 3. Metrics Tab
with tab3:
    st.header("Job Metrics")
    res = requests.get(f"{api_url}/jobs/{job_id}/metrics")
    if res.status_code == 200:
        metrics = res.json()
        
        # Duration Chart
        durations = metrics.get("stage_durations", {})
        if durations:
            st.subheader("Stage Durations (seconds)")
            df_dur = pd.DataFrame(list(durations.items()), columns=["Stage", "Duration"])
            st.bar_chart(df_dur.set_index("Stage"))
            
        # Size Chart
        sizes = metrics.get("artifact_sizes", {})
        if sizes:
            st.subheader("Artifact Sizes (bytes)")
            df_size = pd.DataFrame(list(sizes.items()), columns=["Artifact", "Size"])
            st.bar_chart(df_size.set_index("Artifact"))
            
        st.json(metrics)
    else:
        st.info("Metrics not available.")

# 4. Evidence Tab
with tab4:
    st.header("Evidence Pack")
    res = requests.get(f"{api_url}/jobs/{job_id}/evidence")
    if res.status_code == 200:
        manifest = res.json()
        st.write("The following evidence artifacts have been packaged:")
        df = pd.DataFrame(manifest)
        st.dataframe(df)
        
        st.info("Artifacts can be downloaded from the API or output directory directly.")
    else:
        st.info("Evidence manifest not found. It may still be generating or the job hasn't reached this step.")
