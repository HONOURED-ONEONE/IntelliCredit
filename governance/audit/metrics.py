import json
from pathlib import Path
from datetime import datetime, timezone
import os
from loguru import logger
import sys

def setup_job_logger(job_dir: Path, job_id: str, cfg: dict):
    # Remove default handler
    logger.remove()
    
    log_config = cfg.get("governance", {}).get("logging", {})
    json_fmt = log_config.get("json", True)
    rotation_mb = log_config.get("rotation_mb", 10)
    retention_runs = log_config.get("retention_runs", 20)
    
    log_file = job_dir / "logs.jsonl"
    
    if json_fmt:
        logger.add(str(log_file), serialize=True, rotation=f"{rotation_mb} MB", retention=retention_runs)
    else:
        logger.add(str(log_file), rotation=f"{rotation_mb} MB", retention=retention_runs)
        
    logger.add(sys.stderr, level="INFO")
    return logger.bind(job_id=job_id)

def collect_metrics(job_dir: Path):
    metrics_file = job_dir / "metrics.json"
    prov_file = job_dir / "provenance.json"
    
    stage_durations = {}
    if prov_file.exists():
        with open(prov_file, "r", encoding="utf-8") as f:
            prov = json.load(f)
            for stage, data in prov.get("stages", {}).items():
                start = datetime.fromisoformat(data["started_at"])
                end = datetime.fromisoformat(data["ended_at"])
                stage_durations[stage] = (end - start).total_seconds()
                
    artifact_sizes = {}
    artifacts = [
        "ingestor/facts.jsonl", 
        "research/research_findings.jsonl", 
        "primary/risk_arguments.jsonl", 
        "decision_engine/decision_output.json"
    ]
    for art in artifacts:
        p = job_dir / art
        if p.exists():
            artifact_sizes[art.split('/')[-1]] = p.stat().st_size
            
    # Mock tokens
    tokens = {
        "prompt": 0,
        "completion": 0,
        "total": 0
    }
    
    errors = 0
    stages = ["ingestor", "research", "primary", "decision"]
    for s in stages:
        vr = job_dir / f"{s}_validation_report.json"
        if vr.exists():
            with open(vr, "r", encoding="utf-8") as f:
                rep = json.load(f)
                errors += rep.get("summary", {}).get("critical", 0)
                
    metrics = {
        "stage_durations": stage_durations,
        "artifact_sizes": artifact_sizes,
        "tokens": tokens,
        "errors": errors
    }
    
    with open(metrics_file, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
