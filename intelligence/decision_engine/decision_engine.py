import json
from pathlib import Path
from loguru import logger
from governance.validation.validators import write_validation_report

def run(job_dir: Path, cfg: dict, payload: dict) -> None:
    out_dir = job_dir / "decision_engine"
    out_dir.mkdir(parents=True, exist_ok=True)

    signals_file = job_dir / "ingestor" / "signals.json"
    mismatch = False
    if signals_file.exists():
        with open(signals_file, "r") as f:
            signals = json.load(f)
            mismatch = signals.get("mismatch", False)

    research_file = job_dir / "research" / "research_findings.jsonl"
    adverse_count = 0
    if research_file.exists():
        with open(research_file, "r") as f:
            for line in f:
                if line.strip():
                    item = json.loads(line)
                    if item.get("stance") == "adverse":
                        adverse_count += 1

    primary_file = job_dir / "primary" / "risk_arguments.jsonl"
    total_delta = 0
    if primary_file.exists():
        with open(primary_file, "r") as f:
            for line in f:
                if line.strip():
                    item = json.loads(line)
                    total_delta += item.get("proposed_delta", 0)
    
    total_delta = max(-10, min(10, total_delta))

    score = 60
    drivers = []

    if mismatch:
        score -= 10
        drivers.append("GST vs Bank mismatch detected (-10)")
    if adverse_count > 0:
        score -= 15
        drivers.append(f"Adverse media mentions: {adverse_count} (-15)")
    if total_delta != 0:
        score += total_delta
        drivers.append(f"Officer notes impact ({total_delta:+d})")

    limit = max(500000.0, min(5000000.0, score * 25000.0))
    rate = max(9.0, min(16.0, 16.0 - (score - 50) * 0.1))
    decision = "Approved" if score >= 40 else "Rejected"

    out_json = {
        "decision": decision,
        "limit": float(limit),
        "rate": float(rate),
        "drivers": drivers
    }
    with open(out_dir / "decision_output.json", "w") as f:
        json.dump(out_json, f, indent=2)

    score_breakdown = {
        "base_score": 60,
        "final_score": score,
        "adjustments": drivers
    }
    with open(out_dir / "score_breakdown.json", "w") as f:
        json.dump(score_breakdown, f, indent=2)

    cam_md = f"""# Credit Approval Memo (CAM)
## Decision: {decision}
**Limit:** ₹{limit:,.2f}  
**Rate:** {rate:.2f}%  
**Final Score:** {score}

## Drivers
"""
    for d in drivers:
        cam_md += f"- {d}
"
        
    cam_md += """
## Artifacts Referenced
- `ingestor/signals.json`
- `research/research_findings.jsonl`
- `primary/risk_arguments.jsonl`
"""

    with open(out_dir / "cam.md", "w") as f:
        f.write(cam_md)

    write_validation_report("decision_engine", out_dir, {"decision": decision})
