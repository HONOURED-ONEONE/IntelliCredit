import pytest
import json
from pathlib import Path
from intelligence.decision_engine.decision_engine import run as run_decision

def test_config_driven_decision(tmp_path):
    job_dir = tmp_path / "job_1"
    out_dir = job_dir / "decision_engine"
    out_dir.mkdir(parents=True)
    
    cfg = {
        "decision": {
            "base_score": 50,
            "adjustments": {"gst_bank_mismatch": -5, "adverse_per_item": -10, "primary_delta_cap": 5},
            "pricing": {"min_rate": 8.0, "max_rate": 15.0, "slope_per_score": 0.2, "min_limit": 100000, "max_limit": 10000000, "k_limit": 20000}
        },
        "gates": {"missing_data_policy": "ALLOW"}
    }
    
    # create dummy inputs
    job_dir.joinpath("ingestor").mkdir(parents=True)
    with open(job_dir / "ingestor" / "signals.json", "w") as f:
        json.dump({"mismatch": True, "mismatch_value": 100}, f)
        
    job_dir.joinpath("research").mkdir(parents=True)
    with open(job_dir / "research" / "research_findings.jsonl", "w") as f:
        f.write(json.dumps({"stance": "adverse"}) + "
")
        
    job_dir.joinpath("primary").mkdir(parents=True)
    with open(job_dir / "primary" / "impact_report.json", "w") as f:
        json.dump({"weighted_total_delta": 10}, f)
        
    # Validation so gates pass
    with open(job_dir / "ingestor_validation_report.json", "w") as f:
        json.dump({"summary": {"critical": 0}, "issues": []}, f)
        
    # Touch facts.jsonl so missing data policy ALLOW is valid
    with open(job_dir / "facts.jsonl", "w") as f:
        pass
        
    run_decision(job_dir, cfg, {})
    
    with open(out_dir / "decision_output.json", "r") as f:
        res = json.load(f)
        
    # Base 50 - 5 (mismatch) - 10 (adverse) + 5 (cap of delta 10) = 40
    # Score = 40
    # Rate = 15.0 - (40-50)*0.2 = 15.0 - (-2) = 17.0 -> max_rate 15.0 -> rate 15.0
    # Limit = 40 * 20000 = 800000
    assert res["decision"] == "Approved"
    assert res["limit"] == 800000.0
    assert res["rate"] == 15.0

def test_gate_triggered_refer(tmp_path):
    job_dir = tmp_path / "job_2"
    out_dir = job_dir / "decision_engine"
    out_dir.mkdir(parents=True)
    
    cfg = {"decision": {}, "gates": {"missing_data_policy": "REFER"}}
    
    # Critical issue in validation
    with open(job_dir / "ingestor_validation_report.json", "w") as f:
        json.dump({"summary": {"critical": 1}, "issues": [{"severity": "CRITICAL", "message": "Test"}]}, f)
        
    run_decision(job_dir, cfg, {})
    
    with open(out_dir / "decision_output.json", "r") as f:
        res = json.load(f)
        
    assert res["decision"] == "REFER"
    assert any("Gate Triggered" in d for d in res["drivers"])
