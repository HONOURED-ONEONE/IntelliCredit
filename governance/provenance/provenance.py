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
        
    try:
        from datetime import datetime, timezone
        start_dt = datetime.fromisoformat(started_at)
        end_dt = datetime.fromisoformat(ended_at)
        duration = (end_dt - start_dt).total_seconds()
        
        from governance.observability.prom import observe_stage_duration
        observe_stage_duration(stage, duration)
    except Exception:
        pass

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

def append_search_provenance(job_dir: Path, data: dict):
    prov_file = job_dir / "provenance.json"
    if not prov_file.exists():
        return
        
    with open(prov_file, "r", encoding="utf-8") as f:
        prov = json.load(f)
        
    prov["search_ensemble"] = data
    
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
        
    try:
        from orchestration.job_runner import load_config
        config = load_config()
    except Exception:
        config = {}
        
    if "prompt_tokens" in metrics or "completion_tokens" in metrics:
        usage_dict = {
            "type": "llm",
            "provider": metrics.get("provider", "unknown"),
            "model": metrics.get("model", ""),
            "prompt_tokens": metrics.get("prompt_tokens", 0),
            "completion_tokens": metrics.get("completion_tokens", 0),
            "calls": metrics.get("calls", 1)
        }
    else:
        usage_dict = {
            "type": "search",
            "provider": metrics.get("provider", "unknown"),
            "calls": metrics.get("calls", 1)
        }
        
    try:
        from governance.provenance.metrics_append import append_usage
        append_usage(job_dir, config, usage_dict)
    except Exception as e:
        pass
