import json
from pathlib import Path

def aggregate_reports(job_dir: Path) -> dict:
    stages = ["ingestor", "research", "primary", "decision"]
    aggregate = {
        "summary": {"ok": 0, "warn": 0, "critical": 0},
        "per_stage": {},
        "issues": []
    }
    
    for stage in stages:
        report_path = job_dir / f"{stage}_validation_report.json"
        if not report_path.exists():
            continue
            
        with open(report_path, "r", encoding="utf-8") as f:
            try:
                report = json.load(f)
            except json.JSONDecodeError:
                continue
                
        aggregate["per_stage"][stage] = report
        
        summary = report.get("summary", {})
        aggregate["summary"]["ok"] += summary.get("ok", 0)
        aggregate["summary"]["warn"] += summary.get("warn", 0)
        aggregate["summary"]["critical"] += summary.get("critical", 0)
        
        issues = report.get("issues", [])
        aggregate["issues"].extend(issues)
        
    out_path = job_dir / "validation_aggregate.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(aggregate, f, indent=2)
        
    return aggregate
