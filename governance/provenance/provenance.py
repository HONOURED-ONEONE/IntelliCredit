import json
from pathlib import Path
from loguru import logger
from governance.validation.validators import write_validation_report

def update_provenance_timing(job_dir: Path, stage: str, timing_info: dict):
    prov_file = job_dir / "provenance.json"
    if prov_file.exists():
        with open(prov_file, "r") as f:
            prov = json.load(f)
        prov.setdefault("timing", {})[stage] = timing_info
        with open(prov_file, "w") as f:
            json.dump(prov, f, indent=2)

def append_metrics(job_dir: Path, stage: str, metrics: dict):
    metrics_file = job_dir / "metrics.json"
    data = {}
    if metrics_file.exists():
        with open(metrics_file, "r") as f:
            data = json.load(f)
            
    if stage not in data:
        data[stage] = []
    data[stage].append(metrics)
    
    with open(metrics_file, "w") as f:
        json.dump(data, f, indent=2)
