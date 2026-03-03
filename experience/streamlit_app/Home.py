import streamlit as st
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="IntelliCredit - Phase 3", page_icon="🏦")

st.title("Welcome to IntelliCredit")
st.markdown("""
### Phase 3: Live LLM/VLM Integrations & Guardrails

This system now supports optional Live Providers for:
- **Vision Extraction** (GPT-4o)
- **Live Search** (Perplexity)
- **Reasoning-First Primary Insights** (Claude 3.7 Sonnet)
- **Schema Repair & Governance**
""")

st.sidebar.header("Configuration")
api_host = st.sidebar.text_input("API Host", value=os.getenv("API_HOST", "127.0.0.1"))
api_port = st.sidebar.text_input("API Port", value=os.getenv("API_PORT", "8000"))

provider_mode = st.sidebar.selectbox("Provider Mode", ["mock", "local_uploads"], index=0)

st.sidebar.subheader("Live Features")
enable_live_llm = st.sidebar.checkbox("Enable Live LLM", value=False)
enable_live_search = st.sidebar.checkbox("Enable Live Search", value=False)

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

if enable_live_llm or enable_live_search:
    st.sidebar.warning("Live features enabled. API calls will be made.")
else:
    st.sidebar.info("Fully offline/mock mode active.")
