import pytest
from providers.search.perplexity_provider import PerplexityProvider
from unittest.mock import patch, MagicMock

def test_perplexity_citations_parsing():
    cfg = {"security": {"redact_keys_in_logs": False}}
    with patch("os.getenv", return_value="dummy_key"):
        provider = PerplexityProvider(cfg)
    
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "choices": [{"message": {"content": "This is a response [1] and [2]."}}] ,
        "citations": ["http://example.com/1", "http://example.com/2"]
    }
    
    with patch("httpx.Client.post", return_value=mock_resp):
        results = provider.search("test query", 365)
        
    assert len(results) == 2
    assert results[0]["url"] == "http://example.com/1"
    assert results[1]["url"] == "http://example.com/2"

def test_perplexity_fallback_regex():
    cfg = {"security": {"redact_keys_in_logs": False}}
    with patch("os.getenv", return_value="dummy_key"):
        provider = PerplexityProvider(cfg)
        
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "choices": [{"message": {"content": "Check https://test.com/link out."}}]
    }
    
    with patch("httpx.Client.post", return_value=mock_resp):
        results = provider.search("test query", 365)
        
    assert len(results) == 1
    assert results[0]["url"] == "https://test.com/link"
