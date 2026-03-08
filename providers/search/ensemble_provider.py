import concurrent.futures
import json
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from providers.search.base import SearchProvider
from providers.search.url_utils import canonical_url, domain_quality

logger = logging.getLogger(__name__)

class EnsembleSearchProvider(SearchProvider):
    def __init__(self, providers: Dict[str, SearchProvider], config: dict):
        self.providers = providers
        self.config = config
        self.limits = config.get("search", {}).get("limits", {})
        self.timeouts = config.get("search", {}).get("timeouts", {})
        self.fusion = config.get("search", {}).get("fusion", {})
        self.rrf_k = self.fusion.get("rrf_k", 60)
        self.per_provider_timeout = self.timeouts.get("per_provider_s", 8)

    def search(self, query: str, top_k: int = 5, context: dict = None) -> List[dict]:
        """
        Executes a fanned-out search across providers, canonicalizes, deduplicates,
        fuses using RRF, and returns the top merged results.
        """
        if context is None:
            context = {}
            
        max_merged = context.get("max_merged", self.limits.get("max_merged", 10))

        results_by_provider = {}
        fusion_report = {"failures": [], "timeouts": [], "successes": []}

        def _call_provider(name: str, provider: SearchProvider):
            try:
                # pass kwargs if possible, else fallback to just query
                # MockSearchProvider might only accept query
                try:
                    return name, provider.search(query, top_k=top_k)
                except TypeError:
                    return name, provider.search(query)
            except Exception as e:
                logger.error(f"Provider {name} failed: {e}")
                return name, e

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.providers)) as executor:
            future_to_name = {
                executor.submit(_call_provider, name, p): name 
                for name, p in self.providers.items()
            }
            
            for future in concurrent.futures.as_completed(future_to_name, timeout=self.timeouts.get("total_s", 12)):
                name = future_to_name[future]
                try:
                    res_name, res = future.result()
                    if isinstance(res, Exception):
                        fusion_report["failures"].append({"provider": name, "error": str(res)})
                    else:
                        results_by_provider[name] = res
                        fusion_report["successes"].append(name)
                except concurrent.futures.TimeoutError:
                    fusion_report["timeouts"].append(name)
                except Exception as e:
                    fusion_report["failures"].append({"provider": name, "error": str(e)})

        # Normalization
        normalized_results = []
        for provider_name, raw_results in results_by_provider.items():
            for rank, r in enumerate(raw_results, start=1):
                url = r.get("url")
                if not url:
                    continue
                snippet = r.get("snippet") or r.get("content") or ""
                normalized_results.append({
                    "title": r.get("title", ""),
                    "url": url,
                    "snippet": snippet,
                    "date": r.get("date") or r.get("published_date") or None,
                    "provider": provider_name,
                    "provider_rank": rank,
                    "provider_score": r.get("score") or r.get("source_quality", 50),
                    "raw_subset": r
                })

        # Deduplication
        dedup_map = {}
        for nr in normalized_results:
            c_url = canonical_url(nr["url"])
            if c_url not in dedup_map:
                dedup_map[c_url] = {
                    "canonical_url": c_url,
                    "urls": set(),
                    "titles": [],
                    "snippets": [],
                    "dates": [],
                    "providers_hit": [],
                    "rank_by_provider": {},
                    "raw_subsets": {},
                    "domain_quality": domain_quality(nr["url"])
                }
            
            dm = dedup_map[c_url]
            dm["urls"].add(nr["url"])
            dm["titles"].append(nr["title"])
            dm["snippets"].append(nr["snippet"])
            if nr["date"]:
                dm["dates"].append(nr["date"])
            
            prov = nr["provider"]
            if prov not in dm["providers_hit"]:
                dm["providers_hit"].append(prov)
                dm["rank_by_provider"][prov] = nr["provider_rank"]
                dm["raw_subsets"][prov] = nr["raw_subset"]

        # Fusion (RRF)
        fused = []
        for c_url, dm in dedup_map.items():
            rrf_score = sum(1.0 / (self.rrf_k + rank) for prov, rank in dm["rank_by_provider"].items())
            
            # Select best title and snippet
            best_snippet = max(dm["snippets"], key=len) if dm["snippets"] else ""
            best_title = dm["titles"][0] if dm["titles"] else ""
            best_date = dm["dates"][0] if dm["dates"] else None
            best_url = list(dm["urls"])[0]

            fused.append({
                "query": query,
                "title": best_title,
                "url": best_url,
                "snippet": best_snippet,
                "published_date": best_date,
                "date": best_date,
                "source_provider": "ensemble",
                "providers_hit": dm["providers_hit"],
                "rank_by_provider": dm["rank_by_provider"],
                "rrf_score": rrf_score,
                "domain_quality": dm["domain_quality"],
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                "canonical_url": c_url,
                "_raw_subsets": dm["raw_subsets"] # temporary field
            })

        # Sort with tie-breakers
        fused.sort(key=lambda x: (
            x["rrf_score"],
            len(x["providers_hit"]),
            x["domain_quality"],
            x["published_date"] or "",
            x["canonical_url"]
        ), reverse=True)

        final_fused = fused[:max_merged]
        
        # Attach raw results and report to context so caller can persist them
        context["raw_results"] = results_by_provider
        context["fusion_report"] = fusion_report
        context["dedup_ratio"] = 1.0 - (len(dedup_map) / len(normalized_results)) if normalized_results else 0.0
        
        return final_fused
