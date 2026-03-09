import os
import streamlit as st
import yaml
from pathlib import Path
import requests

def get_api_url() -> str:
    if "api_url" in st.session_state:
        return st.session_state["api_url"]
    
    env_api_url = os.environ.get("API_URL")
    if env_api_url:
        st.session_state["api_url"] = env_api_url
        return env_api_url
    
    # Check if we have API_HOST/API_PORT explicitly set
    host = os.environ.get("API_HOST")
    port = os.environ.get("API_PORT")
    
    if host:
        p = f":{port}" if port else ""
        if host in ["127.0.0.1", "localhost", "0.0.0.0"]:
            url = f"http://{host}{p}"
        else:
            url = f"https://{host}{p}"
        st.session_state["api_url"] = url
        return url
        
    fallback = "https://intellicredit-live.up.railway.app"
    st.session_state["api_url"] = fallback
    return fallback

def get_provider_mode() -> str:
    if "provider_mode" in st.session_state:
        return st.session_state["provider_mode"]
    
    env_mode = os.environ.get("PROVIDER_MODE")
    if env_mode:
        st.session_state["provider_mode"] = env_mode
        return env_mode
    
    try:
        config_path = Path("config/base.yaml")
        if config_path.exists():
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            if config and "providers" in config and "mode" in config["providers"]:
                mode = config["providers"]["mode"]
                st.session_state["provider_mode"] = mode
                return mode
    except Exception:
        pass
        
    st.session_state["provider_mode"] = "mock"
    return "mock"

def fetch_artifact(api_url: str, job_id: str, path: str):
    try:
        res = requests.get(f"{api_url}/jobs/{job_id}/artifact", params={"path": path})
        if res.status_code == 200:
            if "application/json" in res.headers.get("Content-Type", ""):
                return res.json()
            return res.text
        return None
    except Exception:
        return None
