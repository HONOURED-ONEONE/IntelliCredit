import pytest
from intelligence.research.research_agent import run
import json

def test_search_risk_escalation(tmp_path):
    # If adverse keywords in results, should trigger escalation
    job_dir = tmp_path / "job_risk"
    cfg = {
        "features": {"enable_live_search": True},
        "search": {
            "mode": "ensemble",
            "providers": ["mock"],
            "limits": {
                "max_merged": 2,
                "max_merged_high_risk": 5,
                "top_k_per_provider": 2,
                "top_k_per_provider_high_risk": 5,
                "risk_escalation_enabled": True
            },
            "risk_escalation": {
                "threshold": 1,
                "adverse_keywords": ["fraud"],
                "weights": {
                    "adverse_hit": 1,
                    "legal_hit": 1
                }
            }
        }
    }
    
    payload = {"company_name": "FraudCorp"}
    
    import providers.search.mock_provider as mp
    original_search = mp.MockSearchProvider.search
    
    def mock_search(self, q, top_k=5, **kwargs):
        # return results with "fraud"
        return [{"title": f"Fraud {i}", "url": f"http://fraud.com/{i}", "snippet": "fraud"} for i in range(top_k)]
    
    mp.MockSearchProvider.search = mock_search
    try:
        run(job_dir, cfg, payload)
        
        # Check risk escalation
        risk_file = job_dir / "secondary_research" / "risk_escalation.json"
        assert risk_file.exists()
        with open(risk_file) as f:
            risk = json.load(f)
            q_hash = list(risk.keys())[0]
            assert risk[q_hash]["triggered"] is True
            assert risk[q_hash]["final_limits"] == 5
            
        fused_file = job_dir / "secondary_research" / "fused_results.jsonl"
        with open(fused_file) as f:
            lines = f.readlines()
            assert len(lines) == 5 # Because it expanded to 5
    finally:
        mp.MockSearchProvider.search = original_search

def test_search_no_risk_escalation(tmp_path):
    job_dir = tmp_path / "job_no_risk"
    cfg = {
        "features": {"enable_live_search": True},
        "search": {
            "mode": "ensemble",
            "providers": ["mock"],
            "limits": {
                "max_merged": 2,
                "max_merged_high_risk": 5,
                "top_k_per_provider": 2,
                "top_k_per_provider_high_risk": 5,
                "risk_escalation_enabled": True
            },
            "risk_escalation": {
                "threshold": 100, # impossible
            }
        }
    }
    payload = {"company_name": "SafeCorp"}
    
    import providers.search.mock_provider as mp
    original_search = mp.MockSearchProvider.search
    
    def mock_search(self, q, top_k=5, **kwargs):
        return [{"title": f"Safe {i}", "url": f"http://safe.com/{i}", "snippet": "safe"} for i in range(top_k)]
    
    mp.MockSearchProvider.search = mock_search
    try:
        run(job_dir, cfg, payload)
        
        risk_file = job_dir / "secondary_research" / "risk_escalation.json"
        assert risk_file.exists()
        with open(risk_file) as f:
            risk = json.load(f)
            q_hash = list(risk.keys())[0]
            assert risk[q_hash]["triggered"] is False
            assert risk[q_hash]["final_limits"] == 2
            
        fused_file = job_dir / "secondary_research" / "fused_results.jsonl"
        with open(fused_file) as f:
            lines = f.readlines()
            assert len(lines) == 2 # Did not expand
    finally:
        mp.MockSearchProvider.search = original_search
