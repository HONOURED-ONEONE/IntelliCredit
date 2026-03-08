# experience/streamlit_app/pages/6_Settings.py
import os
import json
import requests
import streamlit as st
from pathlib import Path
import yaml

st.set_page_config(page_title="Settings", page_icon="⚙️")
st.title("Settings")

# Read current API endpoint from session (as set in Home.py)
api_url = st.session_state.get("api_url", f"http://{os.getenv('API_HOST', '127.0.0.1')}:{os.getenv('API_PORT', '8000')}")

st.subheader("Backend Readiness")
colA, colB = st.columns(2)
try:
    r = requests.get(f"{api_url}/health/ready", timeout=4)
    if r.status_code == 200:
        ready = r.json()
        colA.success("Backend: Ready")
        colB.code(json.dumps(ready, indent=2), language="json")
    else:
        colA.error(f"Backend Not Ready (HTTP {r.status_code})")
except Exception as e:
    colA.error(f"Backend not reachable: {e}")

st.markdown("---")

st.subheader("Runtime Configuration (from YAML)")
# Load config/base.yaml for display
project_root = Path(__file__).resolve().parent.parent.parent.parent
config_path = project_root / "config" / "base.yaml"
if config_path.exists():
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    with st.expander("Current config/base.yaml (read-only)"):
        st.code(yaml.safe_dump(cfg, sort_keys=False), language="yaml")
else:
    st.info("config/base.yaml not found in the current deployment.")

st.markdown("---")

st.subheader("API Keys (Session Only)")
with st.form("keys_form"):
    openai_key = st.text_input("OpenAI API Key", value=os.getenv("OPENAI_API_KEY", ""), type="password")
    anthropic_key = st.text_input("Anthropic API Key", value=os.getenv("ANTHROPIC_API_KEY", ""), type="password")
    pplx_key = st.text_input("Perplexity API Key", value=os.getenv("PPLX_API_KEY", ""), type="password")
    tavily_key = st.text_input("Tavily API Key", value=os.getenv("TAVILY_API_KEY", ""), type="password")
    serpapi_key = st.text_input("SerpAPI API Key", value=os.getenv("SERPAPI_API_KEY", ""), type="password")

    submitted = st.form_submit_button("Apply for this session")
    if submitted:
        # Store into process env for the running Streamlit session, as done in Home.py
        if openai_key: os.environ["OPENAI_API_KEY"] = openai_key
        if anthropic_key: os.environ["ANTHROPIC_API_KEY"] = anthropic_key
        if pplx_key: os.environ["PPLX_API_KEY"] = pplx_key
        if tavily_key: os.environ["TAVILY_API_KEY"] = tavily_key
        if serpapi_key: os.environ["SERPAPI_API_KEY"] = serpapi_key
        st.success("Keys applied to this session. New jobs will use these values (if live features are enabled).")

st.markdown("---")

st.subheader("Live Features (Session Flags)")
# Keep in sync with Home.py naming
enable_live_llm = st.checkbox("Enable Live LLM", value=st.session_state.get("enable_live_llm", False))
enable_live_search = st.checkbox("Enable Live Search (Perplexity/Tavily/SerpAPI)", value=st.session_state.get("enable_live_search", False))
enable_live_databricks = st.checkbox("Enable Live Databricks", value=st.session_state.get("enable_live_databricks", False))

st.session_state["enable_live_llm"] = enable_live_llm
st.session_state["enable_live_search"] = enable_live_search
st.session_state["enable_live_databricks"] = enable_live_databricks

st.info("These toggles influence the payload/feature flags sent when you create a new job.")