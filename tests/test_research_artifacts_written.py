import pytest
from intelligence.research.research_agent import run
from pathlib import Path

def test_research_artifacts_written(tmp_path):
    job_dir = tmp_path / "job_artifacts"
    cfg = {
        "features": {"enable_live_search": True},
        "search": {
            "mode": "ensemble",
            "providers": ["mock"]
        }
    }
    payload = {"company_name": "TestCorp"}
    
    run(job_dir, cfg, payload)
    
    sec_res_dir = job_dir / "secondary_research"
    assert (sec_res_dir / "fused_results.jsonl").exists()
    assert (sec_res_dir / "fusion_report.json").exists()
    assert (sec_res_dir / "risk_escalation.json").exists()
    
    raw_dir = sec_res_dir / "raw"
    assert raw_dir.exists()
    
    mock_files = list(raw_dir.glob("provider_mock_*.json"))
    assert len(mock_files) > 0
