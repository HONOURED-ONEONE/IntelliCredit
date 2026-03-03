import json
import os
from pathlib import Path
from loguru import logger
from governance.validation.validators import write_validation_report
from governance.provenance.provenance import append_metrics

def run(job_dir: Path, cfg: dict, payload: dict) -> None:
    out_dir = job_dir / "decision_engine"
    out_dir.mkdir(parents=True, exist_ok=True)

    signals_file = job_dir / "ingestor" / "signals.json"
    mismatch = False
    mismatch_val = 0.0
    if signals_file.exists():
        with open(signals_file, "r") as f:
            signals = json.load(f)
            mismatch = signals.get("mismatch", False)
            mismatch_val = signals.get("mismatch_value", 0.0)

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
    quotes = []
    if primary_file.exists():
        with open(primary_file, "r") as f:
            for line in f:
                if line.strip():
                    item = json.loads(line)
                    if not item.get("note_missing_quote"):
                        quotes.append(item.get("quote", ""))
                        
    impact_report_file = job_dir / "primary" / "impact_report.json"
    total_delta = 0
    if impact_report_file.exists():
        with open(impact_report_file, "r") as f:
            impact = json.load(f)
            total_delta = impact.get("weighted_total_delta", impact.get("total_delta", 0))

    dec_cfg = cfg.get("decision", {})
    base_score = dec_cfg.get("base_score", 60)
    adj_cfg = dec_cfg.get("adjustments", {})
    pricing_cfg = dec_cfg.get("pricing", {})
    
    cap = adj_cfg.get("primary_delta_cap", 10)
    total_delta = max(-cap, min(cap, total_delta))

    score = base_score
    drivers = []

    if mismatch:
        penalty = adj_cfg.get("gst_bank_mismatch", -10)
        score += penalty
        drivers.append(f"GST vs Bank mismatch detected (magnitude: {mismatch_val:,.2f}) ({penalty})")
    if adverse_count > 0:
        penalty = adverse_count * adj_cfg.get("adverse_per_item", -15)
        score += penalty
        drivers.append(f"Adverse media mentions: {adverse_count} ({penalty})")
    if total_delta != 0:
        score += total_delta
        drivers.append(f"Officer notes impact ({total_delta:+d})")

    min_limit = pricing_cfg.get("min_limit", 500000.0)
    max_limit = pricing_cfg.get("max_limit", 5000000.0)
    k_limit = pricing_cfg.get("k_limit", 25000.0)
    
    min_rate = pricing_cfg.get("min_rate", 9.0)
    max_rate = pricing_cfg.get("max_rate", 16.0)
    slope = pricing_cfg.get("slope_per_score", 0.1)

    limit = max(min_limit, min(max_limit, score * k_limit))
    rate = max(min_rate, min(max_rate, max_rate - (score - 50) * slope))
    decision = "Approved" if score >= 40 else "Rejected"

    from governance.guardrails.policies import apply_gates
    gate_res = apply_gates(job_dir, cfg)
    if gate_res.get("action") != "ALLOW":
        decision = "REFER"
        for r in gate_res.get("reasons", []):
            drivers.append(f"Gate Triggered: {r}")

    out_json = {
        "decision": decision,
        "limit": float(limit),
        "rate": float(rate),
        "drivers": drivers
    }
    with open(out_dir / "decision_output.json", "w") as f:
        json.dump(out_json, f, indent=2)

    score_breakdown = {
        "base_score": base_score,
        "final_score": score,
        "adjustments": drivers
    }
    with open(out_dir / "score_breakdown.json", "w") as f:
        json.dump(score_breakdown, f, indent=2)

    enable_live_llm = payload.get("enable_live_llm", cfg.get("features", {}).get("enable_live_llm", False))
    cam_md = ""

    if enable_live_llm and os.getenv("OPENAI_API_KEY"):
        try:
            from providers.llm.openai_client import OpenAIClient
            client = OpenAIClient(api_key=os.getenv("OPENAI_API_KEY"))
            
            prompt = f"""Generate a professional Credit Approval Memo (CAM) in Markdown format based on the following deterministic results:
Decision: {decision}
Limit: ₹{limit:,.2f}
Rate: {rate:.2f}%
Final Score: {score}

Drivers:
{json.dumps(drivers)}

Quotes from Officer:
{json.dumps(quotes)}

Include sections for Decision Summary, Risk Drivers, and Artifacts Referenced. Limit to 300 words."""
            
            # Use json schema wrapping a markdown string
            schema = {
                "type": "object",
                "properties": {"markdown": {"type": "string"}},
                "required": ["markdown"]
            }
            
            res, metrics = client.complete_json(prompt, schema, model="gpt-4o-mini", max_tokens=1000)
            append_metrics(job_dir, "decision_narrative", metrics)
            cam_md = res.get("markdown", "")
        except Exception as e:
            logger.error(f"CAM generation failed: {e}")

    if not cam_md:
        cam_md = f"""# Credit Approval Memo (CAM)
## Decision: {decision}
**Limit:** ₹{limit:,.2f}  
**Rate:** {rate:.2f}%  
**Final Score:** {score}

## Drivers
"""
        for d in drivers:
            cam_md += f"- {d}\n"
            
        cam_md += """
## Artifacts Referenced
- `ingestor/signals.json`
- `research/research_findings.jsonl`
- `primary/risk_arguments.jsonl`
"""

    with open(out_dir / "cam.md", "w") as f:
        f.write(cam_md)

    write_validation_report("decision_engine", out_dir, {"decision": decision})
    
    if payload.get("export", True):
        from .export import cam_to_docx, cam_to_pdf
        cam_to_docx(job_dir)
        cam_to_pdf(job_dir)
