import json
import os
from pathlib import Path
from loguru import logger
from providers.search.mock_provider import MockSearchProvider
from providers.search.perplexity_provider import PerplexityProvider
from governance.validation.validators import write_validation_report
from governance.provenance.provenance import append_metrics

def run(job_dir: Path, cfg: dict, payload: dict) -> None:
    out_dir = job_dir / "research"
    out_dir.mkdir(parents=True, exist_ok=True)

    company_name = payload.get("company_name", "")
    promoter = payload.get("promoter", "")
    enable_live_search = payload.get("enable_live_search", cfg.get("features", {}).get("enable_live_search", False))
    
    queries = []
    if company_name:
        queries.append(f"{company_name} adverse media fraud litigation")
    if promoter:
        queries.append(f"{promoter} default OR investigation")
    
    if not queries:
        queries = ["Company adverse news"]

    if enable_live_search and os.getenv("PPLX_API_KEY"):
        provider = PerplexityProvider(api_key=os.getenv("PPLX_API_KEY"))
        logger.info("Using Perplexity live search")
    else:
        provider = MockSearchProvider()
        logger.info("Using Mock search")
    
    findings = []
    seen_urls = set()
    adverse_keywords = ["fraud", "default", "litigation", "investigation", "adverse", "penalty", "scam"]

    for q in queries:
        # Mock doesn't take freshness_days, but let's pass dummy or handle in wrapper
        try:
            results = provider.search(q) if isinstance(provider, MockSearchProvider) else provider.search(q, freshness_days=365)
            # Log a fake metric for search
            append_metrics(job_dir, "research", {"query": q, "results": len(results)})
        except Exception as e:
            logger.error(f"Search failed: {e}")
            results = []

        for r in results:
            url = r.get("url")
            if url in seen_urls: continue
            seen_urls.add(url)
            
            text = f"{r.get('title','')} {r.get('snippet','')}".lower()
            stance = "adverse" if any(k in text for k in adverse_keywords) else "neutral"
            
            findings.append({
                "entity": q.split()[0], # rough entity
                "claim": r.get("title") or r.get("snippet")[:100],
                "stance": stance,
                "citations": [r]
            })

    with open(out_dir / "research_findings.jsonl", "w") as f:
        for finding in findings:
            f.write(json.dumps(finding) + "\n")

    summary_lines = ["# Research Summary\n"]
    for f_item in findings:
        stance_str = f"**[{f_item['stance'].upper()}]**"
        cits = f_item['citations'][0]
        summary_lines.append(f"- {stance_str} {f_item['claim']} ([Source]({cits.get('url', '#')})) - {cits.get('date', 'Unknown Date')}")

    with open(out_dir / "research_summary.md", "w") as f:
        f.write("\n".join(summary_lines))

    write_validation_report("research", out_dir, {"findings_count": len(findings)})
