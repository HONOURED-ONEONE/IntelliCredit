import json
from pathlib import Path
from loguru import logger
from providers.search.mock_provider import MockSearchProvider
from governance.validation.validators import write_validation_report

def run(job_dir: Path, cfg: dict, payload: dict) -> None:
    out_dir = job_dir / "research"
    out_dir.mkdir(parents=True, exist_ok=True)

    company_name = payload.get("company_name", "")
    promoter = payload.get("promoter", "")
    queries = []
    if company_name: queries.append(company_name)
    if promoter: queries.append(promoter)
    
    if not queries:
        queries = ["Company A"]

    provider = MockSearchProvider()
    
    findings = []
    seen_urls = set()
    adverse_keywords = ["fraud", "default", "litigation"]

    for q in queries:
        results = provider.search(q)
        for r in results:
            url = r.get("url")
            if url in seen_urls: continue
            seen_urls.add(url)
            
            text = f"{r.get('title','')} {r.get('snippet','')}".lower()
            stance = "adverse" if any(k in text for k in adverse_keywords) else "neutral"
            
            findings.append({
                "entity": q,
                "claim": r.get("title"),
                "stance": stance,
                "citations": [r]
            })

    with open(out_dir / "research_findings.jsonl", "w") as f:
        for finding in findings:
            f.write(json.dumps(finding) + "
")

    summary_lines = ["# Research Summary
"]
    for f_item in findings:
        stance_str = f"**[{f_item['stance'].upper()}]**"
        cits = f_item['citations'][0]
        summary_lines.append(f"- {stance_str} {f_item['claim']} ([Source]({cits['url']}))")

    with open(out_dir / "research_summary.md", "w") as f:
        f.write("
".join(summary_lines))

    write_validation_report("research", out_dir, {"findings_count": len(findings)})
