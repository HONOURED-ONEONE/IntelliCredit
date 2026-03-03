import streamlit as st
import requests
import time
import json
import pandas as pd
import altair as alt
from pathlib import Path

st.set_page_config(page_title="Ingest Job", page_icon="📥")
st.title("1. Ingest Data & Start Job")

api_url = st.session_state.get("api_url", "http://127.0.0.1:8000")
provider_mode = st.session_state.get("provider_mode", "mock")

with st.form("job_form"):
    company_name = st.text_input("Company Name", "Sample Company")
    promoter = st.text_input("Promoter Name", "John Doe")
    notes = st.text_area("Officer Notes (Include a quote in \"\" for primary insights)", 'The client has a "strong" track record.')
    
    gst_file = None
    bank_file = None
    if provider_mode == "local_uploads":
        st.subheader("Upload Inputs")
        gst_file = st.file_uploader("GST Returns CSV", type=["csv"])
        bank_file = st.file_uploader("Bank Transactions CSV", type=["csv"])
        
    start_button = st.form_submit_button("Start Job")

if start_button:
    payload = {
        "company_name": company_name,
        "promoter": promoter,
        "notes": notes,
        "provider_mode": provider_mode,
        "source": "streamlit"
    }
    
    try:
        res = requests.post(f"{api_url}/jobs", json=payload)
        res.raise_for_status()
        job_id = res.json()["job_id"]
        st.session_state["current_job_id"] = job_id
        st.success(f"Job started! ID: {job_id}")
        
        if provider_mode == "local_uploads":
            import yaml
            project_root = Path(__file__).resolve().parent.parent.parent.parent
            config_path = project_root / "config" / "base.yaml"
            output_root = "outputs/jobs"
            if config_path.exists():
                with open(config_path, "r") as cf:
                    conf = yaml.safe_load(cf)
                    output_root = conf.get("paths", {}).get("output_root", "outputs/jobs")
            job_dir = project_root / output_root / job_id
            inputs_dir = job_dir / "inputs"
            inputs_dir.mkdir(parents=True, exist_ok=True)
            if gst_file:
                with open(inputs_dir / "gst_returns.csv", "wb") as f:
                    f.write(gst_file.getbuffer())
            if bank_file:
                with open(inputs_dir / "bank_transactions.csv", "wb") as f:
                    f.write(bank_file.getbuffer())
                    
    except Exception as e:
        st.error(f"Failed to start job: {e}")

if "current_job_id" in st.session_state:
    job_id = st.session_state["current_job_id"]
    st.markdown(f"### Monitoring Job: `{job_id}`")
    
    status_ph = st.empty()
    
    completed = False
    for _ in range(15):
        try:
            s_res = requests.get(f"{api_url}/jobs/{job_id}")
            if s_res.status_code == 200:
                stage = s_res.json()["stage"]
                status_ph.info(f"**Current Stage:** {stage}")
                if stage == "completed":
                    status_ph.success("**Job Completed!**")
                    completed = True
                    break
        except requests.exceptions.RequestException:
            pass
        time.sleep(1)

    st.markdown("### Ingestor Preview")
    import yaml
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    config_path = project_root / "config" / "base.yaml"
    output_root = "outputs/jobs"
    if config_path.exists():
        with open(config_path, "r") as cf:
            conf = yaml.safe_load(cf)
            output_root = conf.get("paths", {}).get("output_root", "outputs/jobs")
    job_dir = project_root / output_root / job_id
    ingestor_dir = job_dir / "ingestor"
    
    if ingestor_dir.exists():
        gst_path = ingestor_dir / "gst_returns.csv"
        bank_path = ingestor_dir / "bank_transactions.csv"
        
        if gst_path.exists():
            df_gst = pd.read_csv(gst_path)
            st.write("GST Returns (Top 10)")
            st.dataframe(df_gst.head(10))
            
        if bank_path.exists():
            df_bank = pd.read_csv(bank_path)
            st.write("Bank Transactions (Top 10)")
            st.dataframe(df_bank.head(10))

        facts_path = ingestor_dir / "facts.jsonl"
        if facts_path.exists():
            facts = []
            with open(facts_path, "r") as f:
                for line in f:
                    if line.strip(): facts.append(json.loads(line))
            if facts:
                df_facts = pd.DataFrame(facts)
                st.write("Extracted Facts")
                st.dataframe(df_facts)
                
                if not df_facts.empty and 'field' in df_facts.columns and 'value' in df_facts.columns:
                    chart = alt.Chart(df_facts).mark_bar().encode(
                        x='field', y='value', color='field'
                    ).properties(width=500, height=300)
                    st.altair_chart(chart)
