import streamlit as st
import os

st.set_page_config(page_title="IntelliCredit - Phase 1", page_icon="🏦")

st.title("Welcome to IntelliCredit")
st.markdown("""
### Phase 1: Bootstrapping (Minimal End‑to‑End Job Runner)

This is the initial minimal UI for the IntelliCredit system.
Currently, the system handles:
- Job Orchestration
- Basic Provenance & Logging
- Artifact Storage

Navigate to the **Ingest** page using the sidebar to start a new job.
""")

st.sidebar.header("Configuration")
api_host = st.sidebar.text_input("API Host", value=os.getenv("API_HOST", "127.0.0.1"))
api_port = st.sidebar.text_input("API Port", value=os.getenv("API_PORT", "8000"))

st.session_state["api_url"] = f"http://{api_host}:{api_port}"
