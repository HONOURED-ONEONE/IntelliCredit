# experience/streamlit_app/pages/1_Ingest.py
import streamlit as st
import requests
import time
import json
import pandas as pd
import altair as alt
from pathlib import Path
import yaml

st.set_page_config(page_title="Ingest Data & Start Job", page_icon="📥")
st.title("1. Ingest Data & Start Job")

api_url = st.session_state.get("api_url", "http://127.0.0.1:8000")
provider_mode = st.session_state.get("provider_mode", "mock")

with st.form("job_form"):
    company_name = st.text_input("Company Name", "")
    promoter = st.text_input("Promoter Name", "")

    gst_file = None
    bank_file = None
    pdf_files = []

    dbfs_path = ""
    catalog = ""
    schema = ""
    gst_table = ""
    bank_table = ""

    if provider_mode == "local_uploads":
        st.subheader("Upload Inputs")
        gst_file = st.file_uploader("GST Returns CSV", type=["csv"])
        bank_file = st.file_uploader("Bank Transactions CSV", type=["csv"])
        # Keep documents to PDF per current API contract (pdfs field)
        pdf_files = st.file_uploader("Financial Docs (PDF)", type=["pdf"], accept_multiple_files=True)

    elif provider_mode == "databricks_files":
        st.subheader("Databricks Files")
        dbfs_path = st.text_input("DBFS Path", value="dbfs:/Shared/credit_docs")

    elif provider_mode == "databricks_tables":
        st.subheader("Databricks Tables")
        catalog = st.text_input("Catalog", value="main")
        schema = st.text_input("Schema", value="credit")
        gst_table = st.text_input("GST Table Name", value="gst_returns")
        bank_table = st.text_input("Bank Table Name", value="bank_transactions")

    start_button = st.form_submit_button("Start Job")

if start_button:
    payload = {
        "company_name": company_name,
        "promoter": promoter,
        # No officer notes here anymore (moved to Primary Insights page)
        "source_mode": provider_mode,
        "enable_live_llm": st.session_state.get("enable_live_llm", False),
        "enable_live_search": st.session_state.get("enable_live_search", False),
        "enable_live_databricks": st.session_state.get("enable_live_databricks", False),
        "dbfs_path": dbfs_path,
        "catalog": catalog,
        "schema": schema,
        "gst_table": gst_table,
        "bank_table": bank_table,
        "source": "streamlit"
    }

    try:
        res = requests.post(f"{api_url}/jobs", json=payload)
        res.raise_for_status()
        job_id = res.json()["job_id"]
        st.session_state["current_job_id"] = job_id
        st.success(f"Job started! ID: {job_id}")

        if provider_mode == "local_uploads":
            files_to_send = []
            if gst_file:
                files_to_send.append(("gst_returns", (gst_file.name, gst_file.getvalue(), "text/csv")))
            if bank_file:
                files_to_send.append(("bank_transactions", (bank_file.name, bank_file.getvalue(), "text/csv")))
            if pdf_files:
                for pf in pdf_files:
                    files_to_send.append(("pdfs", (pf.name, pf.getvalue(), "application/pdf")))
            if files_to_send:
                u_res = requests.post(f"{api_url}/jobs/{job_id}/uploads", files=files_to_send)
                u_res.raise_for_status()
                st.info(f"Uploaded {len(u_res.json()['saved'])} files.")

    except Exception as e:
        st.error(f"Failed to start job: {e}")

if "current_job_id" in st.session_state:
    job_id = st.session_state["current_job_id"]
    st.markdown(f"### Monitoring Job: `{job_id}`")
    status_ph = st.empty()

    for _ in range(30):  # a bit longer to allow live paths
        try:
            s_res = requests.get(f"{api_url}/jobs/{job_id}")
            if s_res.status_code == 200:
                stage = s_res.json()["stage"]
                status_ph.info(f"**Current Stage:** {stage}")
                if stage == "completed":
                    status_ph.success("**Job Completed!**")
                    break
        except requests.exceptions.RequestException:
            pass
        time.sleep(1)

    st.markdown("### Ingestor Preview")
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
        facts_path = ingestor_dir / "facts.jsonl"
        if facts_path.exists():
            facts = []
            with open(facts_path, "r") as f:
                for line in f:
                    if line.strip():
                        facts.append(json.loads(line))
            if facts:
                df_facts = pd.DataFrame(facts)
                st.write("Extracted Facts (Includes Vision LLM if enabled)")
                st.dataframe(df_facts)