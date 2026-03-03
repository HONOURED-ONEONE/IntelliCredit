import json
from pathlib import Path

def apply_gates(job_dir: Path, cfg: dict) -> dict:
    action = "ALLOW"
    reasons = []
    
    stages = ["ingestor", "research", "primary", "decision"]
    
    # Check for critical issues in any validation report
    for stage in stages:
        report_path = job_dir / f"{stage}_validation_report.json"
        if report_path.exists():
            with open(report_path, "r", encoding="utf-8") as f:
                try:
                    report = json.load(f)
                    if report.get("summary", {}).get("critical", 0) > 0:
                        action = "REFER"
                        for issue in report.get("issues", []):
                            if issue.get("severity", "").upper() == "CRITICAL":
                                reasons.append(f"[{stage}] {issue.get('message')}")
                except Exception as e:
                    action = "REFER"
                    reasons.append(f"Failed to read validation report for {stage}: {e}")
                    
    # Check configured missing data policy and missing artifacts
    # Here we simulate basic check, in reality it should look at the output logic
    # Assume facts.jsonl must exist after ingestor
    if not (job_dir / "ingestor" / "facts.jsonl").exists() and (job_dir / "ingestor_validation_report.json").exists():
        missing_policy = cfg.get("gates", {}).get("missing_data_policy", "REFER")
        if missing_policy != "ALLOW":
            action = missing_policy
            reasons.append("Missing critical artifact: facts.jsonl")
            
    # Remove duplicates
    reasons = list(dict.fromkeys(reasons))
    
    # If blocked, it takes precedence
    if any("block" in r.lower() for r in reasons):
        action = "BLOCK"
        
    return {
        "action": action,
        "reasons": reasons
    }