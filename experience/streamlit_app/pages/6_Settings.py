# experience/streamlit_app/pages/06_Settings.py
import os
import json
import requests
import streamlit as st

st.set_page_config(page_title="Settings • Health", page_icon="🩺", layout="wide")
st.title("Settings — Health Only")

# Resolve API URL from session (set in Home.py) or env
def _default_api_url() -> str:
    host = os.getenv("API_HOST", "127.0.0.1")
    port = os.getenv("API_PORT", "8000")
    return f"http://{host}:{port}"

api_url = st.session_state.get("api_url", _default_api_url())

with st.expander("Connection", expanded=True):
    st.text_input("API URL", value=api_url, key="api_url", help="Read-only usage in this page; change on Home if needed.")

st.markdown("---")

col1, col2 = st.columns([1, 2], gap="large")

with col1:
    st.subheader("Backend Readiness")
    try:
        r = requests.get(f"{st.session_state['api_url']}/health/ready", timeout=4)
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
        r = requests.get(f"{st.session_state['api_url']}/health/ready", timeout=4)
        if r.status_code == 200:
            st.code(json.dumps(r.json(), indent=2), language="json")
        else:
            st.info("No readiness JSON available.")
    except Exception as e:
        st.info(f"Could not fetch readiness JSON: {e}")