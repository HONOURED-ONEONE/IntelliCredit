import pytest
from providers.search.ensemble_provider import EnsembleSearchProvider
from providers.search.base import SearchProvider
from governance.validation.validators import validate_research
import json

class FakeProvider(SearchProvider):
    def __init__(self, results):
        self.results = results
    def search(self, query: str, top_k: int = 5, **kwargs) -> list:
        return self.results

class FailingProvider(SearchProvider):
    def search(self, query: str, top_k: int = 5, **kwargs) -> list:
        raise ValueError("Timeout")

def test_ensemble_partial_failure(tmp_path):
    p1 = FakeProvider([{"title": "A", "url": "http://a.com", "snippet": "a"}])
    p2 = FakeProvider([{"title": "B", "url": "http://b.com", "snippet": "b"}])
    p3 = FailingProvider()
    
    ensemble = EnsembleSearchProvider({"p1": p1, "p2": p2, "p3": p3}, {})
    
    context = {}
    results = ensemble.search("query", context=context)
    
    assert len(results) == 2
    assert "p3" in context["fusion_report"]["failures"][0]["provider"]
    assert len(context["fusion_report"]["successes"]) == 2
    
    # Check validation warns, not critical
    job_dir = tmp_path / "job_1"
    sec_res_dir = job_dir / "secondary_research"
    sec_res_dir.mkdir(parents=True)
    
    with open(sec_res_dir / "fused_results.jsonl", "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")
            
    with open(sec_res_dir / "fusion_report.json", "w") as f:
        json.dump({"query_hash": context["fusion_report"]}, f)
        
    # Create findings for validation schema
    research_dir = job_dir / "research"
    research_dir.mkdir()
    with open(research_dir / "research_findings.jsonl", "w") as f:
        f.write(json.dumps({
            "entity": "company", 
            "claim": "Analysis", 
            "stance": "neutral", 
            "entity_confidence": 0.5, 
            "legal_hits": 0, 
            "citations": []
        }) + "\n")
        
    report = validate_research(job_dir)
    assert any(i["severity"] == "WARN" and i["code"] in ["DEGRADED_COVERAGE", "PARTIAL_FAILURE"] for i in report["issues"])
    assert not any(i["severity"] == "CRITICAL" for i in report["issues"])
