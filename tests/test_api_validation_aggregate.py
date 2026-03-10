import json
import pytest
from fastapi.testclient import TestClient
from experience.api.main import app
import experience.api.main

client = TestClient(app)

def test_get_validation_aggregate(monkeypatch, tmp_path):
    monkeypatch.setattr(experience.api.main, "project_root", tmp_path)
    
    def mock_load_config():
        return {"paths": {"output_root": "outputs/jobs"}}
        
    monkeypatch.setattr(experience.api.main, "load_config", mock_load_config)
    
    job_id = "test-job-123"
    job_dir = tmp_path / "outputs/jobs" / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    
    response = client.get(f"/jobs/{job_id}/validation/aggregate")
    assert response.status_code == 404
    
    agg_file = job_dir / "validation_aggregate.json"
    test_data = {"overall_status": "PASS", "stages": {}}
    with open(agg_file, "w") as f:
        json.dump(test_data, f)
        
    response = client.get(f"/jobs/{job_id}/validation/aggregate")
    assert response.status_code == 200
    assert response.json() == test_data
