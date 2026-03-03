import pytest
from providers.search.url_utils import canonical_url, domain_quality
from providers.search.indiankanoon_provider import IndianKanoonProvider
from intelligence.research.research_agent import run
from pathlib import Path
import json

def test_canonical_url():
    url = "https://example.com/path?utm_source=test&other=1#fragment"
    assert canonical_url(url) == "https://example.com/path?other=1"

def test_domain_quality():
    assert domain_quality("https://rbi.org.in/test") == 20
    assert domain_quality("https://www.reuters.com/article") == 20
    assert domain_quality("https://random.com") == 0

def test_indiankanoon_provider_enabled():
    cfg = {"search": {"legal_sources": {"indiankanoon": {"enabled": True}}}}
    provider = IndianKanoonProvider(cfg)
    results = provider.search("Test Company")
    assert len(results) == 1
    assert "indiankanoon.org" in results[0]["url"]
    assert results[0]["source_quality"] == 85

def test_indiankanoon_provider_disabled():
    cfg = {"search": {"legal_sources": {"indiankanoon": {"enabled": False}}}}
    provider = IndianKanoonProvider(cfg)
    assert len(provider.search("Test Company")) == 0

def test_research_agent_disambiguation_and_summary(tmp_path):
    job_dir = tmp_path / "job_1"
    
    cfg = {
        "features": {"enable_live_search": False},
        "search": {
            "provider": "mock",
            "legal_sources": {"indiankanoon": {"enabled": True}}
        }
    }
    
    payload = {
        "company_name": "TestCorp",
        "promoter": "John Doe"
    }
    
    run(job_dir, cfg, payload)
    
    findings_file = job_dir / "research" / "research_findings.jsonl"
    assert findings_file.exists()
    
    findings = []
    with open(findings_file, "r") as f:
        for line in f:
            findings.append(json.loads(line))
            
    assert len(findings) == 2
    assert findings[0]["entity"] == "company"
    assert findings[1]["entity"] == "promoter"
    
    summary_file = job_dir / "research" / "research_summary.md"
    assert summary_file.exists()
    
    with open(summary_file, "r") as f:
        content = f.read()
        
    assert "John Doe" in content or "TestCorp" in content
    assert "SQ:" in content
