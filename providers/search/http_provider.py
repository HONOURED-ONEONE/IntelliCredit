import os
import httpx
from datetime import datetime, timezone
from typing import List
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

class HttpSearchProvider:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.tavily_key = os.getenv("TAVILY_API_KEY")
        self.bing_key = os.getenv("BING_SUBSCRIPTION_KEY")
        
    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=5))
    def search(self, query: str, freshness_days: int) -> List[dict]:
        if self.tavily_key:
            return self._search_tavily(query)
        elif self.bing_key:
            return self._search_bing(query)
        else:
            logger.warning("No TAVILY_API_KEY or BING_SUBSCRIPTION_KEY provided, returning empty")
            return []

    def _search_tavily(self, query: str) -> List[dict]:
        url = "https://api.tavily.com/search"
        payload = {
            "api_key": self.tavily_key,
            "query": query,
            "search_depth": "basic",
            "include_answer": False,
            "max_results": 5
        }
        try:
            with httpx.Client(timeout=20.0) as client:
                resp = client.post(url, json=payload)
                resp.raise_for_status()
                data = resp.json()
                results = []
                for res in data.get("results", []):
                    sq = 80 if "gov" in res.get("url", "") or "reuters" in res.get("url", "") else 60
                    results.append({
                        "title": res.get("title", ""),
                        "url": res.get("url", ""),
                        "snippet": res.get("content", ""),
                        "date": datetime.now(timezone.utc).isoformat(),
                        "source_quality": sq
                    })
                return results
        except Exception as e:
            if self.cfg.get("security", {}).get("redact_keys_in_logs", True):
                logger.error("Tavily search failed")
            else:
                logger.error(f"Tavily search failed: {e}")
            return []

    def _search_bing(self, query: str) -> List[dict]:
        url = "https://api.bing.microsoft.com/v7.0/search"
        headers = {"Ocp-Apim-Subscription-Key": self.bing_key}
        params = {"q": query, "count": 5}
        try:
            with httpx.Client(timeout=20.0) as client:
                resp = client.get(url, headers=headers, params=params)
                resp.raise_for_status()
                data = resp.json()
                results = []
                for res in data.get("webPages", {}).get("value", []):
                    sq = 80 if "gov" in res.get("url", "") or "reuters" in res.get("url", "") else 60
                    results.append({
                        "title": res.get("name", ""),
                        "url": res.get("url", ""),
                        "snippet": res.get("snippet", ""),
                        "date": datetime.now(timezone.utc).isoformat(),
                        "source_quality": sq
                    })
                return results
        except Exception as e:
            if self.cfg.get("security", {}).get("redact_keys_in_logs", True):
                logger.error("Bing search failed")
            else:
                logger.error(f"Bing search failed: {e}")
            return []
