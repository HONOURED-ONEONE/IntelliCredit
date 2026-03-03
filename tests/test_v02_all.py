import pytest
import json
import pandas as pd
import os
from pathlib import Path
from fastapi.testclient import TestClient
from experience.api.main import app

client = TestClient(app)

def test_api_uploads():
    # 1. Create job via API
    res = client.post("/jobs", json={"source": "test", "company_name": "TestCorp"})
    assert res.status_code == 200
    job_id = res.json()["job_id"]
    
    # 2. Upload files
    gst_csv = "date,sales,tax_paid\n2023-01-01,1000,180\n2023-02-01,5000,900"
    bank_csv = "date,description,amount\n2023-01-01,Deposit,1000\n2023-02-01,Withdraw,-4500"
    
    files = [
        ("gst_returns", ("gst.csv", gst_csv, "text/csv")),
        ("bank_transactions", ("bank.csv", bank_csv, "text/csv")),
        ("pdfs", ("test1.pdf", b"%PDF-1.4 test1", "application/pdf")),
        ("pdfs", ("test2.pdf", b"%PDF-1.4 test2", "application/pdf"))
    ]
    
    response = client.post(f"/jobs/{job_id}/uploads", files=files)
    if response.status_code != 200:
        print(response.json())
    assert response.status_code == 200
    data = response.json()
    assert len(data["saved"]) == 4

def test_decision_policy_matrix(tmp_path):
    job_dir = tmp_path / "job_policy"
    job_dir.mkdir(parents=True)
    (job_dir / "ingestor").mkdir()
    (job_dir / "research").mkdir()
    (job_dir / "research" / "entities").mkdir()
    
    # 1. Test Circular Trading Policy
    signals = {
        "circular_trading_risk": {"score": 70, "drivers": ["High risk"]}
    }
    with open(job_dir / "ingestor" / "signals.json", "w") as f:
        json.dump(signals, f)
        
    cfg = {
        "decision": {
            "base_score": 60,
            "policy_matrix": [
                {"when": {"circular_trading_risk_gte": 60}, "action": "REFER", "driver": "suggests circular trading"}
            ],
            "adjustments": {},
            "pricing": {}
        }
    }
    
    from intelligence.decision_engine import decision_engine
    decision_engine.run(job_dir, cfg, {})
    
    with open(job_dir / "decision_engine" / "decision_output.json", "r") as f:
        res = json.load(f)
        assert res["decision"] == "REFER"
        assert any("suggests circular trading" in d for d in res["drivers"])

def test_research_entity_confidence(tmp_path):
    job_dir = tmp_path / "job_research"
    job_dir.mkdir(parents=True)
    
    from intelligence.research import research_agent
    
    payload = {
        "company_name": "TestCorp",
        "promoter": "Jane Doe",
        "parameters": {
            "company_aliases": ["TC"],
            "promoter_aliases": ["JD"]
        }
    }
    
    cfg = {
        "search": {"provider": "mock", "freshness_days": 365},
        "features": {"enable_live_search": False}
    }
    
    research_agent.run(job_dir, cfg, payload)
    assert (job_dir / "research" / "entities" / "profile.json").exists()
    with open(job_dir / "research" / "entities" / "profile.json", "r") as f:
        prof = json.load(f)
        assert "company" in prof
        assert "promoter" in prof
        assert prof["company"]["entity_confidence"] >= 0
