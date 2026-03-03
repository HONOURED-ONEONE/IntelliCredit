import json
import os
import hashlib
import urllib.parse
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone
from providers.search.mock_provider import MockSearchProvider
from providers.search.perplexity_provider import PerplexityProvider
from providers.search.http_provider import HttpSearchProvider
from providers.search.url_utils import canonical_url, domain_quality
from providers.search.indiankanoon_provider import IndianKanoonProvider

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
        queries.append((f"{company_name} adverse media fraud litigation", "company", company_name))
    if promoter:
        queries.append((f"{promoter} default OR investigation", "promoter", promoter))
    
    if not queries:
        queries = [("Company adverse news", "company", "Company")]

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
        
    ik_provider = IndianKanoonProvider(cfg)
    
    findings = []
    seen_urls = set()
    adverse_keywords = ["fraud", "default", "litigation", "investigation", "adverse", "penalty", "scam"]

    for q, e_type, e_name in queries:
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
                
                if not isinstance(provider, MockSearchProvider):
                    logger.info("Degrading to mock provider for this query")
                    mock_prov = MockSearchProvider()
                    results = mock_prov.search(q)

        for r in results:
            text_to_search = (r.get("title", "") + " " + r.get("snippet", "")).lower()
            if e_type == "promoter":
                score = 50
                if e_name.lower() in text_to_search:
                    score += 30
                if company_name and company_name.lower() in text_to_search:
                    score += 20
                r["disambiguation_score"] = min(100, score)
            else:
                r["disambiguation_score"] = 100 if e_name.lower() in text_to_search else 50

        unique_results = {}
        for r in results:
            url = r.get("url", "")
            if not url: continue
            
            c_url = canonical_url(url)
            if c_url in unique_results: continue
            
            sq = r.get("source_quality", 50)
            sq += domain_quality(url)
            r["source_quality"] = max(0, min(100, sq))
            
            date_val = r.get("date", "")
            if not date_val:
                r["date"] = datetime.now(timezone.utc).isoformat()
                
            unique_results[c_url] = r
            
        capped_results = list(unique_results.values())
        
        ik_results = ik_provider.search(e_name)
        for r in ik_results:
            c_url = canonical_url(r["url"])
            if c_url not in unique_results:
                capped_results.append(r)
                
        capped_results = capped_results[:5]

        if capped_results:
            text = " ".join([f"{r.get('title','')} {r.get('snippet','')}" for r in capped_results]).lower()
            stance = "adverse" if any(k in text for k in adverse_keywords) else "neutral"
            
            findings.append({
                "entity": e_type,
                "claim": f"Analysis for {e_name}",
                "stance": stance,
                "citations": capped_results
            })

    with open(out_dir / "research_findings.jsonl", "w", encoding="utf-8") as f:
        for finding in findings:
            f.write(json.dumps(finding) + "\n")

    summary_lines = ["# Research Summary\n"]
    for f_item in findings:
        stance_str = f"**[{f_item['stance'].upper()}]**"
        for cit in f_item['citations']:
            host = urllib.parse.urlparse(cit.get('url', '')).netloc
            summary_lines.append(f"- {stance_str} {f_item['claim']} ([{host}]({cit.get('url', '#')})) - SQ: {cit.get('source_quality', 0)}")

    with open(out_dir / "research_summary.md", "w", encoding="utf-8") as f:
        f.write("\n".join(summary_lines))
