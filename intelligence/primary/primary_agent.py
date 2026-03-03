import json
import re
from pathlib import Path
from loguru import logger
from governance.validation.validators import write_validation_report

def run(job_dir: Path, cfg: dict, payload: dict) -> None:
    out_dir = job_dir / "primary"
    out_dir.mkdir(parents=True, exist_ok=True)

    notes = payload.get("notes", "")
    arguments = []
    missing_quote = False

    match = re.search(r'"([^"]*)"', notes)
    if match:
        quote = match.group(1)
        obs = "Quoted observation found"
    else:
        quote = "No explicit quote provided"
        obs = "Inferred from notes"
        missing_quote = True

    five_c_mapping = {
        "fraud": "Character",
        "default": "Capacity",
        "collateral": "Collateral",
        "market": "Conditions"
    }

    five_c = "Capacity"
    notes_lower = notes.lower()
    for k, v in five_c_mapping.items():
        if k in notes_lower:
            five_c = v
            break

    delta = 0
    if "good" in notes_lower or "strong" in notes_lower: delta = 5
    if "bad" in notes_lower or "weak" in notes_lower: delta = -5

    if notes.strip():
        arguments.append({
            "quote": quote,
            "observation": obs,
            "interpretation": "Analyzed from officer notes",
            "five_c": five_c,
            "proposed_delta": delta,
            "freshness_weight": 1.0,
            "note_missing_quote": missing_quote
        })

    with open(out_dir / "risk_arguments.jsonl", "w") as f:
        for arg in arguments:
            f.write(json.dumps(arg) + "
")

    impact_report = {
        "arguments_count": len(arguments),
        "missing_quote_flags": sum(1 for a in arguments if a.get("note_missing_quote", False)),
        "total_delta": sum(a["proposed_delta"] for a in arguments)
    }

    with open(out_dir / "impact_report.json", "w") as f:
        json.dump(impact_report, f, indent=2)

    write_validation_report("primary", out_dir, {"arguments_count": len(arguments)})
