import os
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ----------------------------
# Page config
# ----------------------------
st.set_page_config(
    page_title="IntelliCredit - MVP",
    page_icon="🏦",
    layout="wide"
)

# ----------------------------
# Helper: get API URL
# ----------------------------
def _default_api_url() -> str:
    host = os.getenv("API_HOST", "127.0.0.1")
    port = os.getenv("API_PORT", "8000")
    return f"http://{host}:{port}"

if "api_url" not in st.session_state:
    st.session_state["api_url"] = _default_api_url()

api_url = st.session_state["api_url"]

# ----------------------------
# Header
# ----------------------------
st.title("IntelliCredit MVP")
st.markdown(
    """
Welcome to the IntelliCredit MVP.  
Use the **Settings** page to configure API keys and live features. Then proceed to **Ingest**, **Research**, **Primary Insights**, and **Decision** to run end‑to‑end jobs.
"""
)

# ----------------------------
# API Endpoint (editable)
# ----------------------------
with st.expander("Connection"):
    st.text_input("API URL", value=api_url, key="api_url")
    st.caption("Tip: this persists for the current session. The default comes from `API_HOST` and `API_PORT`.")

# ----------------------------
# Backend readiness
# ----------------------------
col1, col2 = st.columns([1, 2], gap="large")
with col1:
    st.subheader("Backend Status")

    try:
        r = requests.get(f"{st.session_state['api_url']}/health/ready", timeout=3)
        if r.status_code == 200:
            st.success("Backend: Ready 🟢")
            ready = r.json()
            write_ok = ready.get("write_access", False)
            st.write(f"- Write access: **{'OK' if write_ok else 'Not available'}**")
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
    st.subheader("Quick Links")
    st.markdown(
        """
- **Settings** → Configure keys & live features
- **1. Ingest Data & Start Job** → Upload CSVs/PDFs or point to Databricks
- **2. Secondary Research** → View research findings & validation
- **3. Primary Insights** → Add officer notes and view arguments
- **4. Decision Engine** → CAM, decision output, and scores
- **5. Validation & Provenance** → Cross-stage validation, metrics, evidence
        """
    )

    st.divider()
    st.subheader("Shortcuts")
    cols = st.columns(3)
    cols[0].page_link("pages/0_Settings.py", label="⚙️ Settings", icon="⚙️")
    cols[1].page_link("pages/1_Ingest.py", label="📥 Ingest", icon="📥")
    cols[2].page_link("pages/2_Research.py", label="🔎 Research", icon="🔎")

    cols2 = st.columns(3)
    cols2[0].page_link("pages/3_PrimaryInsights.py", label="💡 Primary Insights", icon="💡")
    cols2[1].page_link("pages/4_Decision.py", label="⚖️ Decision", icon="⚖️")
    cols2[2].page_link("pages/5_Validation_Provenance.py", label="🛡️ Validation & Provenance", icon="🛡️")

# ----------------------------
# Footer / tips
# ----------------------------
st.markdown("---")
st.caption(
    "Tip: Start in **Settings** to enable live providers (OpenAI/Anthropic/Search/Databricks). "
    "All keys are session‑scoped; store them securely in your deployment environment for production."
)