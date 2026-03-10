import json
import pytest
from fastapi.testclient import TestClient
from experience.api.main import app
import experience.api.main

client = TestClient(app)

def test_get_artifact(monkeypatch, tmp_path):
    monkeypatch.setattr(experience.api.main, "project_root", tmp_path)
    
    def mock_load_config():
        return {"paths": {"output_root": "outputs/jobs"}}
        
    monkeypatch.setattr(experience.api.main, "load_config", mock_load_config)
    
    job_id = "test-job-artifact"
    job_dir = tmp_path / "outputs/jobs" / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Job not found
    response = client.get("/jobs/missing-job/artifact?path=some/file.txt")
    assert response.status_code == 404
    
    # 2. File not found
    response = client.get(f"/jobs/{job_id}/artifact?path=missing_file.json")
    assert response.status_code == 404
    
    # 3. Path traversal blocked
    (tmp_path / "outputs" / "secret.txt").write_text("secret")
    response = client.get(f"/jobs/{job_id}/artifact?path=../../secret.txt")
    assert response.status_code == 403
    
    # 4. JSON file successful return
    test_json = {"key": "value"}
    json_path = job_dir / "data.json"
    with open(json_path, "w") as f:
        json.dump(test_json, f)
        
    response = client.get(f"/jobs/{job_id}/artifact?path=data.json")
    assert response.status_code == 200
    assert response.json() == test_json
    
    # 5. Text file successful return
    text_path = job_dir / "text.txt"
    text_path.write_text("hello world")
    
    response = client.get(f"/jobs/{job_id}/artifact?path=text.txt")
    assert response.status_code == 200
    assert response.text == "hello world"
