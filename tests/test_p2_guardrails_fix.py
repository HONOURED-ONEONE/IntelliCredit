import pytest
import json
from pathlib import Path
from governance.guardrails.policies import apply_gates

def test_apply_gates_facts_jsonl_found(tmp_path):
    # Setup job directory with facts.jsonl in ingestor subdir
    job_dir = tmp_path / "job_fixed"
    ingestor_dir = job_dir / "ingestor"
    ingestor_dir.mkdir(parents=True)
    
    facts_file = ingestor_dir / "facts.jsonl"
    with open(facts_file, "w") as f:
        f.write(json.dumps({"fact": "test"}) + "\n")
        
    # Also need the validation report to trigger the check
    report_file = job_dir / "ingestor_validation_report.json"
    with open(report_file, "w") as f:
        json.dump({"summary": {"critical": 0}, "issues": []}, f)
        
    cfg = {"gates": {"missing_data_policy": "REFER"}}
    
    result = apply_gates(job_dir, cfg)
    
    assert result["action"] == "ALLOW"
    assert "Missing critical artifact: facts.jsonl" not in result["reasons"]

def test_apply_gates_facts_jsonl_missing(tmp_path):
    # Setup job directory without facts.jsonl
    job_dir = tmp_path / "job_missing"
    job_dir.mkdir(parents=True)
    
    # Still need the validation report to trigger the check
    report_file = job_dir / "ingestor_validation_report.json"
    with open(report_file, "w") as f:
        json.dump({"summary": {"critical": 0}, "issues": []}, f)
        
    cfg = {"gates": {"missing_data_policy": "REFER"}}
    
    result = apply_gates(job_dir, cfg)
    
    assert result["action"] == "REFER"
    assert "Missing critical artifact: facts.jsonl" in result["reasons"]

def test_apply_gates_facts_jsonl_missing_allow_policy(tmp_path):
    # Setup job directory without facts.jsonl
    job_dir = tmp_path / "job_missing_allow"
    job_dir.mkdir(parents=True)
    
    # Still need the validation report to trigger the check
    report_file = job_dir / "ingestor_validation_report.json"
    with open(report_file, "w") as f:
        json.dump({"summary": {"critical": 0}, "issues": []}, f)
        
    cfg = {"gates": {"missing_data_policy": "ALLOW"}}
    
    result = apply_gates(job_dir, cfg)
    
    assert result["action"] == "ALLOW"
    assert "Missing critical artifact: facts.jsonl" not in result["reasons"]
