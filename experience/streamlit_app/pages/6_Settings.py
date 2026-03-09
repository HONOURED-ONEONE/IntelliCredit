# experience/streamlit_app/pages/06_Settings.py
import os
import json
import requests
import streamlit as st
import sys
from pathlib import Path

# Add the parent directory to sys.path to allow importing core_utils
project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from experience.streamlit_app.core_utils import get_api_url

st.set_page_config(page_title="Settings • Health", page_icon="🩺", layout="wide")
st.title("Settings — Health Only")

# Resolve API URL using core_utils
api_url = get_api_url()

with st.expander("Connection", expanded=True):
    st.text_input("API URL", value=api_url, disabled=True, help="Resolved API URL. Edit via environment variables if needed.")

st.markdown("---")

col1, col2 = st.columns([1, 2], gap="large")

with col1:
    st.subheader("Backend Readiness")
    try:
        r = requests.get(f"{api_url}/health/ready", timeout=4)
        if r.status_code == 200:
            ready = r.json()
            st.success("Backend: Ready 🟢")
            st.write("- Write access:", "**OK**" if ready.get("write_access") else "**Not available**")
            llm = ready.get("llm_live", {})
            search = ready.get("search_live", {})
            dbx = ready.get("databricks_live", {})
            st.write(f"- Live LLM: **{'OK' if llm.get('ok') or llm.get('skipped') else 'Missing keys'}**")
            st.write(f"- Live Search: **{'OK' if search.get('ok') or search.get('skipped') else 'Missing keys'}**")
            st.write(f"- Databricks: **{'OK' if dbx.get('ok') or dbx.get('skipped') else 'Missing creds'}**")
        else:
            st.error(f"Backend: Not Ready 🔴 (HTTP {r.status_code})")
    except Exception as e:
        st.error(f"Backend: Disconnected 🔴 ({e})")

with col2:
    st.subheader("Raw Readiness JSON")
    try:
        r = requests.get(f"{api_url}/health/ready", timeout=4)
        if r.status_code == 200:
            st.code(json.dumps(r.json(), indent=2), language="json")
        else:
            st.info("No readiness JSON available.")
    except Exception as e:
        st.info(f"Could not fetch readiness JSON: {e}")