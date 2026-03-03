import json
import os
import hashlib
from pathlib import Path
from loguru import logger
from providers.search.mock_provider import MockSearchProvider
from providers.search.perplexity_provider import PerplexityProvider
from providers.search.http_provider import HttpSearchProvider

def run(job_dir: Path, cfg: dict, payload: dict) -> None:
    out_dir = job_dir / "research"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    cache_dir = out_dir / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    company_name = payload.get("company_name", "")
    promoter = payload.get("promoter", "")
    enable_live_search = payload.get("enable_live_search", cfg.get("features", {}).get("enable_live_search", False))
    provider_name = cfg.get("search", {}).get("provider", "mock")
    freshness_days = cfg.get("search", {}).get("freshness_days", 365)
    
    queries = []
    if company_name:
        queries.append(f"{company_name} adverse media fraud litigation")
    if promoter:
        queries.append(f"{promoter} default OR investigation")
    
    if not queries:
        queries = ["Company adverse news"]

    if enable_live_search:
        if provider_name == "perplexity" and os.getenv("PPLX_API_KEY"):
            provider = PerplexityProvider(cfg)
            logger.info("Using Perplexity live search")
        elif provider_name in ["tavily", "bing"] and (os.getenv("TAVILY_API_KEY") or os.getenv("BING_SUBSCRIPTION_KEY")):
            provider = HttpSearchProvider(cfg)
            logger.info(f"Using {provider_name} live search")
        else:
            logger.warning("Live search requested but keys/provider not matched. Falling back to mock.")
            provider = MockSearchProvider()
    else:
        provider = MockSearchProvider()
        logger.info("Using Mock search")
    
    findings = []
    seen_urls = set()
    adverse_keywords = ["fraud", "default", "litigation", "investigation", "adverse", "penalty", "scam"]

    for q in queries:
        query_hash = hashlib.md5(q.encode()).hexdigest()
        cache_file = cache_dir / f"{query_hash}.json"
        results = []
        
        if cache_file.exists():
            with open(cache_file, "r") as f:
                results = json.load(f)
        else:
            try:
                if isinstance(provider, MockSearchProvider):
                    results = provider.search(q)
                else:
                    results = provider.search(q, freshness_days=freshness_days)
                
                with open(cache_file, "w") as f:
                    json.dump(results, f)
            except Exception as e:
                logger.error(f"Search failed for {q}: {e}")
                
                # degrading to mock on error
                if not isinstance(provider, MockSearchProvider):
                    logger.info("Degrading to mock provider for this query")
                    mock_prov = MockSearchProvider()
                    results = mock_prov.search(q)

        for r in results:
            url = r.get("url", "")
            if url in seen_urls: continue
            seen_urls.add(url)
            
            text = f"{r.get('title','')} {r.get('snippet','')}".lower()
            stance = "adverse" if any(k in text for k in adverse_keywords) else "neutral"
            
            # Ensure dates are valid
            date_val = r.get("date", "")
            if not date_val:
                from datetime import datetime, timezone
                date_val = datetime.now(timezone.utc).isoformat()
                r["date"] = date_val
                
            findings.append({
                "entity": q.split()[0], # rough entity
                "claim": r.get("title") or r.get("snippet")[:100],
                "stance": stance,
                "citations": [r]
            })

    with open(out_dir / "research_findings.jsonl", "w", encoding="utf-8") as f:
        for finding in findings:
            f.write(json.dumps(finding) + "\n")

    summary_lines = ["# Research Summary\n"]
    for f_item in findings:
        stance_str = f"**[{f_item['stance'].upper()}]**"
        cits = f_item['citations'][0]
        summary_lines.append(f"- {stance_str} {f_item['claim']} ([Source]({cits.get('url', '#')})) - {cits.get('date', 'Unknown Date')}")

    with open(out_dir / "research_summary.md", "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines))
