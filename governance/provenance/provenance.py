import json
import sys
import os
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from data_layer.contracts.utils import sha256_of_file

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def get_git_commit():
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL).decode("utf-8").strip()
    except Exception:
        return None

def start_run(job_dir: Path, cfg: dict, payload: dict):
    config_hash = sha256_of_file(Path("config/base.yaml"))
    prov_file = job_dir / "provenance.json"
    
    prov = {
        "run_id": job_dir.name,
        "started_at": _now_iso(),
        "status": "running",
        "environment": {
            "python_version": sys.version,
            "git_commit": get_git_commit()
        },
        "config_snapshot_hash": config_hash,
        "payload": payload,
        "stages": {}
    }
    
    with open(prov_file, "w", encoding="utf-8") as f:
        json.dump(prov, f, indent=2)

def mark_stage(job_dir: Path, stage: str, started_at: str, ended_at: str, outputs: list[Path]):
    prov_file = job_dir / "provenance.json"
    if not prov_file.exists():
        return
        
    with open(prov_file, "r", encoding="utf-8") as f:
        prov = json.load(f)
        
    output_checksums = {}
    for out in outputs:
        if out.exists():
            output_checksums[out.name] = sha256_of_file(out)
            
    prov["stages"][stage] = {
        "started_at": started_at,
        "ended_at": ended_at,
        "outputs": output_checksums
    }
    
    with open(prov_file, "w", encoding="utf-8") as f:
        json.dump(prov, f, indent=2)

def finish_run(job_dir: Path, status: str):
    prov_file = job_dir / "provenance.json"
    if not prov_file.exists():
        return
        
    with open(prov_file, "r", encoding="utf-8") as f:
        prov = json.load(f)
        
    prov["status"] = status
    prov["ended_at"] = _now_iso()
    
    with open(prov_file, "w", encoding="utf-8") as f:
        json.dump(prov, f, indent=2)

def append_metrics(job_dir: Path, namespace: str, metrics: dict):
    metrics_file = job_dir / "metrics.json"
    data = {}
    if metrics_file.exists():
        try:
            with open(metrics_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            pass
            
    data[namespace] = metrics
    with open(metrics_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
