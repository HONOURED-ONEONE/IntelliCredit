import os
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from intelligence.research import research_agent
from providers.search.serpapi_provider import SerpApiProvider

@patch("intelligence.research.research_agent.SerpApiProvider.search")
@patch.dict(os.environ, {"SERPAPI_API_KEY": "test_key"})
def test_provider_selection_serpapi(mock_search, tmp_path):
    cfg = {
        "features": {"enable_live_search": True},
        "search": {"provider": "serpapi", "serpapi": {"engine": "google"}}
    }
    
    payload = {
        "company_name": "TestCorp",
        "parameters": {}
    }
    
    mock_search.return_value = []
    
    # We just need to check if the code runs without raising an exception and tries to use SerpApiProvider.
    # We patch SerpApiProvider.search so it doesn't do a real HTTP call.
    research_agent.run(tmp_path, cfg, payload)
    
    # Since we mocked search, it should have been called if the provider was selected
    assert mock_search.called

@patch("intelligence.research.research_agent.SerpApiProvider.search")
@patch.dict(os.environ, {"SERPAPI_API_KEY": "test_key"})
def test_ensemble_excludes_bing_by_default(tmp_path):
    # This test verifies that the configuration parsed will reflect the new default config where bing is absent
    import yaml
    config_path = Path(__file__).parent.parent / "config" / "base.yaml"
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
        
    assert "bing" not in cfg["search"]["providers"]
    assert "serpapi" in cfg["search"]["providers"]
