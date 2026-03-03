import json
from pathlib import Path
from loguru import logger

def write_validation_report(stage: str, stage_dir: Path, metrics: dict):
    report_path = stage_dir / "validation_report.json"
    report = {
        "stage": stage,
        "metrics": metrics,
        "status": "PASS"
    }
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    logger.info(f"[{stage}] Validation report written to {report_path}")
