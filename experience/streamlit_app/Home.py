import streamlit as st
import os
import requests
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="IntelliCredit - MVP", page_icon="🏦")

st.title("Welcome to IntelliCredit MVP")
st.markdown("""
### Phase 5: Live Databricks & Search Integrations

This system now supports optional Live Providers for:
- **Vision Extraction** (GPT-4o)
- **Live Search** (Perplexity, Tavily, Bing)
- **Reasoning-First Primary Insights** (Claude Sonnet 4.6)
- **Live Databricks** (Files from DBFS/Volumes and Tables from UC)
""")

st.sidebar.header("Configuration")
api_host = st.sidebar.text_input("API Host", value=os.getenv("API_HOST", "127.0.0.1"))
api_port = st.sidebar.text_input("API Port", value=os.getenv("API_PORT", "8000"))

run_profile = st.sidebar.selectbox("Run Profile", ["Local Mock", "Local Uploads", "Databricks Files", "Databricks Tables"], index=0)

profile_map = {
    "Local Mock": "mock",
    "Local Uploads": "local_uploads",
    "Databricks Files": "databricks_files",
    "Databricks Tables": "databricks_tables"
}
provider_mode = profile_map.get(run_profile, "mock")

st.sidebar.subheader("Live Features")
enable_live_llm = st.sidebar.checkbox("Enable Live LLM", value=False)
enable_live_search = st.sidebar.checkbox("Enable Live Search", value=False)
enable_live_databricks = st.sidebar.checkbox("Enable Live Databricks", value=False)

st.sidebar.subheader("API Keys (Session Only)")
openai_key = st.sidebar.text_input("OpenAI API Key", value=os.getenv("OPENAI_API_KEY", ""), type="password")
anthropic_key = st.sidebar.text_input("Anthropic API Key", value=os.getenv("ANTHROPIC_API_KEY", ""), type="password")
pplx_key = st.sidebar.text_input("Perplexity API Key", value=os.getenv("PPLX_API_KEY", ""), type="password")

if openai_key: os.environ["OPENAI_API_KEY"] = openai_key
if anthropic_key: os.environ["ANTHROPIC_API_KEY"] = anthropic_key
if pplx_key: os.environ["PPLX_API_KEY"] = pplx_key

st.session_state["api_url"] = f"http://{api_host}:{api_port}"
st.session_state["provider_mode"] = provider_mode
st.session_state["enable_live_llm"] = enable_live_llm
st.session_state["enable_live_search"] = enable_live_search
st.session_state["enable_live_databricks"] = enable_live_databricks

# Health Readiness Check
api_url = st.session_state["api_url"]
try:
    health_res = requests.get(f"{api_url}/health/ready", timeout=2)
    if health_res.status_code == 200:
        st.sidebar.success("Backend: Ready 🟢")
    else:
        st.sidebar.error("Backend: Not Ready 🔴")
except:
    st.sidebar.error("Backend: Disconnected 🔴")

if enable_live_llm or enable_live_search or enable_live_databricks:
    st.sidebar.warning("Live features enabled. API calls will be made.")
else:
    st.sidebar.info("Fully offline/mock mode active.")

if "current_job_id" in st.session_state:
    st.markdown("---")
    st.subheader(f"Current Job: {st.session_state['current_job_id']}")
    
    job_id = st.session_state["current_job_id"]
    st.write("**Validation Status:**")
    cols = st.columns(4)
    stages = ["ingestor", "research", "primary", "decision"]
    for i, stage in enumerate(stages):
        try:
            res = requests.get(f"{api_url}/jobs/{job_id}/validation?stage={stage}")
            if res.status_code == 200:
                rep = res.json()
                summary = rep.get("summary", {})
                crit = summary.get("critical", 0)
                warn = summary.get("warn", 0)
                ok = summary.get("ok", 0)
                
                if crit > 0:
                    cols[i].error(f"{stage.title()}\nCRIT: {crit}")
                elif warn > 0:
                    cols[i].warning(f"{stage.title()}\nWARN: {warn}")
                else:
                    cols[i].success(f"{stage.title()}\nOK: {ok}")
            else:
                cols[i].info(f"{stage.title()}\nPending")
        except:
            cols[i].info(f"{stage.title()}\nPending")
            
    st.markdown("[Go to Validation & Provenance](/Validation_Provenance)")
