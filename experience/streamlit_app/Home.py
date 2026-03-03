import streamlit as st
import os

st.set_page_config(page_title="IntelliCredit - Phase 2", page_icon="🏦")

st.title("Welcome to IntelliCredit")
st.markdown("""
### Phase 2: Safe Demo Mode

This is the offline, demo-ready IntelliCredit system.
Currently, the system handles:
- Job Orchestration
- Ingestor (CSVs + PDFs)
- Research (Mock Search)
- Primary Insights (Officer Notes)
- Decision Engine (Scoring & Pricing)
""")

st.sidebar.header("Configuration")
api_host = st.sidebar.text_input("API Host", value=os.getenv("API_HOST", "127.0.0.1"))
api_port = st.sidebar.text_input("API Port", value=os.getenv("API_PORT", "8000"))

provider_mode = st.sidebar.selectbox("Provider Mode", ["mock", "local_uploads"], index=0)

st.session_state["api_url"] = f"http://{api_host}:{api_port}"
st.session_state["provider_mode"] = provider_mode

if provider_mode == "local_uploads":
    st.sidebar.info("Local Uploads mode active. You can upload CSV/PDF files on the Ingest page.")
else:
    st.sidebar.info("Mock mode active. Default sample datasets will be used.")
