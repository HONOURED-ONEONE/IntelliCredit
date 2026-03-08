import json
import os
import hashlib
import urllib.parse
import re
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone
from providers.search.mock_provider import MockSearchProvider
from providers.search.perplexity_provider import PerplexityProvider
from providers.search.http_provider import HttpSearchProvider
from providers.search.url_utils import canonical_url, domain_quality
from providers.search.indiankanoon_provider import IndianKanoonProvider
from providers.search.ensemble_provider import EnsembleSearchProvider

def jaccard_similarity(s1, s2):
    # Normalize: lower, strip punctuation
    t1 = set(re.sub(r'[^\w\s]', '', s1.lower()).split())
    t2 = set(re.sub(r'[^\w\s]', '', s2.lower()).split())
    if not t1 or not t2: return 0
    return len(t1.intersection(t2)) / len(t1.union(t2))

def run(job_dir: Path, cfg: dict, payload: dict) -> None:
    out_dir = job_dir / "research"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    entities_dir = out_dir / "entities"
    entities_dir.mkdir(parents=True, exist_ok=True)
    
    cache_dir = out_dir / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    sec_res_dir = job_dir / "secondary_research"
    sec_res_dir.mkdir(parents=True, exist_ok=True)

    company_name = payload.get("company_name", "")
    promoter = payload.get("promoter", "")
    
    params = payload.get("parameters", {})
    company_aliases = params.get("company_aliases", [])
    promoter_aliases = params.get("promoter_aliases", [])
    
    enable_live_search = payload.get("enable_live_search", cfg.get("features", {}).get("enable_live_search", False))
    search_cfg = cfg.get("search", {})
    search_mode = search_cfg.get("mode", "single")
    provider_name = search_cfg.get("provider", "mock")
    ensemble_providers_list = search_cfg.get("providers", ["perplexity", "tavily", "bing"])
    freshness_days = search_cfg.get("freshness_days", 365)
    
    queries = []
    if company_name:
        queries.append((f"{company_name} adverse media fraud litigation", "company", company_name, company_aliases))
    if promoter:
        queries.append((f"{promoter} default OR investigation", "promoter", promoter, promoter_aliases))
    
    if not queries:
        queries = [("Company adverse news", "company", "Company", [])]

    if enable_live_search:
        if search_mode == "ensemble":
            providers_dict = {}
            for p in ensemble_providers_list:
                if p == "perplexity" and os.getenv("PPLX_API_KEY"):
                    providers_dict[p] = PerplexityProvider(cfg)
                elif p == "tavily" and os.getenv("TAVILY_API_KEY"):
                    providers_dict[p] = HttpSearchProvider(cfg)
                elif p == "bing" and os.getenv("BING_SUBSCRIPTION_KEY"):
                    providers_dict[p] = HttpSearchProvider(cfg)
            if not providers_dict:
                providers_dict["mock"] = MockSearchProvider()
            provider = EnsembleSearchProvider(providers_dict, cfg)
        else:
            if provider_name == "perplexity" and os.getenv("PPLX_API_KEY"):
                provider = PerplexityProvider(cfg)
            elif provider_name in ["tavily", "bing"] and (os.getenv("TAVILY_API_KEY") or os.getenv("BING_SUBSCRIPTION_KEY")):
                provider = HttpSearchProvider(cfg)
            else:
                provider = MockSearchProvider()
    else:
        if search_mode == "ensemble":
            provider = EnsembleSearchProvider({"mock": MockSearchProvider()}, cfg)
        else:
            provider = MockSearchProvider()
        
    ik_provider = IndianKanoonProvider(cfg)
    findings = []
    
    entity_profiles = {
        "company": {"canonical_name": company_name, "aliases": company_aliases, "entity_confidence": 0.0, "legal_hits": 0, "top_sources": []},
        "promoter": {"canonical_name": promoter, "aliases": promoter_aliases, "entity_confidence": 0.0, "legal_hits": 0, "top_sources": []}
    }

    adverse_keywords = ["fraud", "default", "litigation", "investigation", "adverse", "penalty", "scam"]

    all_fusion_reports = {}
    all_risk_escalations = {}

    for q, e_type, e_name, aliases in queries:
        query_hash = hashlib.md5(q.encode()).hexdigest()
        cache_file = cache_dir / f"{query_hash}.json"
        results = []
        
        if cache_file.exists() and search_mode != "ensemble":
            with open(cache_file, "r") as f: results = json.load(f)
        else:
            try:
                if search_mode == "ensemble":
                    limits = search_cfg.get("limits", {})
                    max_merged = limits.get("max_merged", 10)
                    top_k_per_provider = limits.get("top_k_per_provider", 5)
                    
                    context = {"max_merged": max_merged}
                    results = provider.search(q, top_k=top_k_per_provider, context=context)
                    
                    risk_cfg = search_cfg.get("risk_escalation", {})
                    risk_threshold = risk_cfg.get("threshold", 3)
                    adv_kws = risk_cfg.get("adverse_keywords", [])
                    leg_kws = risk_cfg.get("legal_keywords", [])
                    domain_hints = risk_cfg.get("domain_hints", {}).get("legal", [])
                    weights = risk_cfg.get("weights", {"adverse_hit": 1, "legal_hit": 1, "low_entity_confidence": 2})
                    
                    score = 0
                    reasons = []
                    
                    for r in results:
                        text = (r.get("title", "") + " " + r.get("snippet", "")).lower()
                        if any(kw.lower() in text for kw in adv_kws):
                            score += weights.get("adverse_hit", 1)
                            reasons.append("adverse_hit")
                        if any(kw.lower() in text for kw in leg_kws):
                            score += weights.get("legal_hit", 1)
                            reasons.append("legal_hit")
                        if any(dh in r.get("url", "").lower() for dh in domain_hints):
                            score += 1
                            reasons.append("legal_domain")
                            
                    triggered = score >= risk_threshold
                    
                    if triggered and limits.get("risk_escalation_enabled", True):
                        max_merged_high = limits.get("max_merged_high_risk", 25)
                        top_k_high = limits.get("top_k_per_provider_high_risk", 10)
                        
                        context_high = {"max_merged": max_merged_high}
                        results = provider.search(q, top_k=top_k_high, context=context_high)
                        context = context_high
                        final_limits_used = max_merged_high
                    else:
                        final_limits_used = max_merged
                        
                    raw_dir = sec_res_dir / "raw"
                    raw_dir.mkdir(parents=True, exist_ok=True)
                    raw_refs = {}
                    for p_name, p_res in context.get("raw_results", {}).items():
                        raw_file = raw_dir / f"provider_{p_name}_{query_hash}.json"
                        with open(raw_file, "w", encoding="utf-8") as f:
                            json.dump(p_res, f, indent=2)
                        raw_refs[p_name] = f"secondary_research/raw/provider_{p_name}_{query_hash}.json"
                        
                    for res in results:
                        res["raw_refs"] = raw_refs
                        res.pop("_raw_subsets", None)

                    with open(sec_res_dir / "fused_results.jsonl", "a", encoding="utf-8") as f:
                        for res in results:
                            f.write(json.dumps(res) + "\n")
                            
                    all_fusion_reports[query_hash] = context.get("fusion_report", {})
                    all_risk_escalations[query_hash] = {
                        "initial_score": score,
                        "threshold": risk_threshold,
                        "triggered": triggered,
                        "reasons": reasons,
                        "final_limits": final_limits_used
                    }
                else:
                    try:
                        results = provider.search(q, freshness_days=freshness_days) if not isinstance(provider, MockSearchProvider) else provider.search(q)
                    except TypeError:
                        results = provider.search(q)
                    with open(cache_file, "w") as f: json.dump(results, f)
            except Exception as e:
                logger.error(f"Search failed: {e}")
                results = MockSearchProvider().search(q)

        legal_hits_count = 0
        ik_results = ik_provider.search(e_name)
        legal_hits_count = len(ik_results)
        entity_profiles[e_type]["legal_hits"] += legal_hits_count

        for r in results:
            text = (r.get("title", "") + " " + r.get("snippet", "")).lower()
            best_sim = jaccard_similarity(e_name, text)
            for al in aliases:
                best_sim = max(best_sim, jaccard_similarity(al, text))
            
            score = best_sim * 80
            if e_name.lower() in text or any(al.lower() in text for al in aliases):
                score += 20
            r["disambiguation_score"] = min(100, score)

        unique_results = {}
        for r in results:
            url = r.get("url", "")
            if not url: continue
            c_url = canonical_url(url)
            if c_url in unique_results: continue
            
            sq = r.get("source_quality", 50) + domain_quality(url)
            r["source_quality"] = max(0, min(100, sq))
            if not r.get("date"): r["date"] = datetime.now(timezone.utc).isoformat()
            unique_results[c_url] = r
            
        final_results = list(unique_results.values())
        for r in ik_results:
            if canonical_url(r["url"]) not in unique_results:
                final_results.append(r)
        
        final_results = sorted(final_results, key=lambda x: (x.get("disambiguation_score", 0), x.get("source_quality", 0)), reverse=True)[:5]

        if final_results:
            mean_d = sum(r.get("disambiguation_score", 50) for r in final_results) / len(final_results)
            mean_sq = sum(r.get("source_quality", 50) for r in final_results) / len(final_results)
            conf = (mean_d / 100) * 0.6 + (mean_sq / 100) * 0.4
            conf = max(0.0, min(1.0, conf))
            
            entity_profiles[e_type]["entity_confidence"] = conf
            for r in final_results:
                entity_profiles[e_type]["top_sources"].append({
                    "url": r.get("url"), "source_quality": r.get("source_quality"), "date": r.get("date")
                })

            all_text = " ".join([f"{r.get('title','')} {r.get('snippet','')}" for r in final_results]).lower()
            stance = "adverse" if any(k in all_text for k in adverse_keywords) else "neutral"
            
            findings.append({
                "entity": e_type,
                "claim": f"Analysis for {e_name}",
                "stance": stance,
                "entity_confidence": round(conf, 2),
                "legal_hits": legal_hits_count,
                "citations": final_results
            })

    if search_mode == "ensemble":
        with open(sec_res_dir / "fusion_report.json", "w", encoding="utf-8") as f:
            json.dump(all_fusion_reports, f, indent=2)
        with open(sec_res_dir / "risk_escalation.json", "w", encoding="utf-8") as f:
            json.dump(all_risk_escalations, f, indent=2)
            
        try:
            from governance.provenance.provenance import append_search_provenance
            append_search_provenance(job_dir, {
                "mode": search_mode,
                "providers": ensemble_providers_list,
                "queries_executed": len(queries),
                "risk_escalation_triggered": any(r.get("triggered") for r in all_risk_escalations.values())
            })
        except Exception: pass

    with open(entities_dir / "profile.json", "w", encoding="utf-8") as f:
        json.dump({**entity_profiles, "generated_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)

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
