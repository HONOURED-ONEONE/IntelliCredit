import json
from pathlib import Path
from data_layer.contracts.registry import validate_artifact
from data_layer.contracts.utils import read_jsonl

def _write_report(job_dir: Path, stage: str, report: dict):
    out_file = job_dir / f"{stage}_validation_report.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

def _init_report():
    return {
        "schema_ok": True,
        "issues": [],
        "counts": {"records": 0, "fields_checked": 0},
        "summary": {"ok": 0, "warn": 0, "critical": 0}
    }

def _add_issue(report: dict, stage: str, severity: str, code: str, message: str, path: str = ""):
    report["issues"].append({
        "id": f"{stage}-{len(report['issues']) + 1}",
        "stage": stage,
        "severity": severity,
        "code": code,
        "message": message,
        "path": path
    })
    if severity.lower() == "critical":
        report["summary"]["critical"] += 1
        report["schema_ok"] = False
    elif severity.lower() == "warn":
        report["summary"]["warn"] += 1
    else:
        report["summary"]["ok"] += 1

def validate_ingestor(job_dir: Path) -> dict:
    report = _init_report()
    stage = "ingestor"
    
    facts_path = job_dir / "ingestor" / "facts.jsonl"
    schema_res = validate_artifact(facts_path, "facts")
    
    if not schema_res["ok"]:
        _add_issue(report, stage, "CRITICAL", "SCHEMA_ERROR", str(schema_res["errors"]), str(facts_path))
    
    facts = read_jsonl(facts_path)
    report["counts"]["records"] = len(facts)
    
    # Logic checks
    revenue = None
    pat = None
    tax = None
    bank_inflow = None
    gst_sales = None
    
    for f in facts:
        report["counts"]["fields_checked"] += 1
        val = f.get("value")
        if isinstance(val, (int, float)) and val < 0 and f.get("field") not in ["PAT"]:
            # Allow PAT to be negative
            _add_issue(report, stage, "WARN", "NEGATIVE_VALUE", f"Negative value for {f.get('field')}", str(facts_path))
            
        field = f.get("field")
        if field == "Revenue":
            revenue = val
        elif field == "PAT":
            pat = val
        elif field == "Tax Paid":
            tax = val
        elif field == "Bank Inflow":
            bank_inflow = val
        elif field == "GST Sales":
            gst_sales = val
            
    if pat is not None and revenue is not None and pat > revenue:
        _add_issue(report, stage, "WARN", "PLAUSIBILITY", "PAT > Revenue", str(facts_path))
    if tax is not None and tax < 0:
        _add_issue(report, stage, "WARN", "PLAUSIBILITY", "Tax Paid < 0", str(facts_path))
    if bank_inflow is not None and gst_sales is not None:
        if isinstance(bank_inflow, (int, float)) and isinstance(gst_sales, (int, float)):
            if abs(bank_inflow - gst_sales) > max(bank_inflow, gst_sales) * 0.5: # 50% tolerance
                _add_issue(report, stage, "WARN", "PLAUSIBILITY", "Large gap between Bank Inflow and GST Sales", str(facts_path))

    _write_report(job_dir, stage, report)
    return report

def validate_research(job_dir: Path) -> dict:
    report = _init_report()
    stage = "research"
    
    findings_path = job_dir / "research" / "research_findings.jsonl"
    schema_res = validate_artifact(findings_path, "research")
    
    if not schema_res["ok"]:
        _add_issue(report, stage, "CRITICAL", "SCHEMA_ERROR", str(schema_res["errors"]), str(findings_path))
        
    findings = read_jsonl(findings_path)
    report["counts"]["records"] = len(findings)
    
    for finding in findings:
        report["counts"]["fields_checked"] += 1
        urls = set()
        for cit in finding.get("citations", []):
            url = cit.get("url")
            if url in urls:
                _add_issue(report, stage, "WARN", "DUPLICATE_URL", f"Duplicate URL: {url}", str(findings_path))
            urls.add(url)
            
            sq = cit.get("source_quality", 0)
            if not (0 <= sq <= 100):
                _add_issue(report, stage, "CRITICAL", "INVALID_SOURCE_QUALITY", f"Source quality out of range: {sq}", str(findings_path))
                
            if not cit.get("date"):
                _add_issue(report, stage, "WARN", "MISSING_DATE", f"Missing date in citation", str(findings_path))
                
    _write_report(job_dir, stage, report)
    return report

def validate_primary(job_dir: Path) -> dict:
    report = _init_report()
    stage = "primary"
    
    # Check what phase 3 uses: risk_arguments.jsonl or primary_insights.jsonl?
    # Phase 3 primary_agent.py writes risk_arguments.jsonl!
    insights_path = job_dir / "primary" / "risk_arguments.jsonl"
    schema_res = validate_artifact(insights_path, "primary")
    
    if not schema_res["ok"]:
        _add_issue(report, stage, "CRITICAL", "SCHEMA_ERROR", str(schema_res["errors"]), str(insights_path))
        
    insights = read_jsonl(insights_path)
    report["counts"]["records"] = len(insights)
    
    for ins in insights:
        report["counts"]["fields_checked"] += 1
        if not ins.get("quote") and ins.get("observation"):
            _add_issue(report, stage, "CRITICAL", "NO_QUOTE_NO_ARGUMENT", "Missing quote for argument", str(insights_path))
            
        delta = ins.get("proposed_delta", 0)
        if not (-10 <= delta <= 10):
            _add_issue(report, stage, "CRITICAL", "DELTA_OUT_OF_RANGE", f"Proposed delta out of range: {delta}", str(insights_path))
            
    _write_report(job_dir, stage, report)
    return report

def validate_decision(job_dir: Path) -> dict:
    report = _init_report()
    stage = "decision"
    
    decision_path = job_dir / "decision_engine" / "decision_output.json"
    schema_res = validate_artifact(decision_path, "decision")
    
    if not schema_res["ok"]:
        if decision_path.exists():
            _add_issue(report, stage, "CRITICAL", "SCHEMA_ERROR", str(schema_res["errors"]), str(decision_path))
        else:
            pass
            
    if decision_path.exists():
        try:
            with open(decision_path, "r", encoding="utf-8") as f:
                dec = json.load(f)
                report["counts"]["records"] = 1
                report["counts"]["fields_checked"] += 2
                
                rate = dec.get("rate", 0)
                limit = dec.get("limit", 0)
                if not (0 <= rate <= 100):
                    _add_issue(report, stage, "CRITICAL", "RATE_OUT_OF_RANGE", f"Rate out of range: {rate}", str(decision_path))
                if limit < 0:
                    _add_issue(report, stage, "CRITICAL", "NEGATIVE_LIMIT", f"Limit < 0: {limit}", str(decision_path))
        except Exception as e:
            _add_issue(report, stage, "CRITICAL", "FILE_READ_ERROR", str(e), str(decision_path))

    _write_report(job_dir, stage, report)
    return report
