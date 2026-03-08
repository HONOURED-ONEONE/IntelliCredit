import os
import requests
from typing import List
from datetime import datetime, timezone
from providers.search.base import SearchProvider
from loguru import logger

class SerpApiProvider(SearchProvider):
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.api_key = os.getenv("SERPAPI_API_KEY")
        serpapi_cfg = self.cfg.get("search", {}).get("serpapi", {})
        self.engine = serpapi_cfg.get("engine", "google")
        self.hl = serpapi_cfg.get("hl", "en")
        self.gl = serpapi_cfg.get("gl", "in")
        self.num = serpapi_cfg.get("num", 10)
        self.safe = serpapi_cfg.get("safe", "off")

    def search(self, query: str, freshness_days: int = None) -> List[dict]:
        if not self.api_key:
            logger.warning("SERPAPI_API_KEY not found, returning empty list")
            return []

        url = "https://serpapi.com/search.json"
        params = {
            "api_key": self.api_key,
            "engine": self.engine,
            "q": query,
            "num": self.num,
        }
        
        if self.engine == "google":
            params["hl"] = self.hl
            params["gl"] = self.gl
            params["safe"] = self.safe

        try:
            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.error(f"SerpAPI search failed for query '{query}': {e}")
            raise e

        organic_results = data.get("organic_results", [])
        results = []

        for item in organic_results:
            title = item.get("title", "")
            link = item.get("link", "")
            snippet = item.get("snippet", "") or item.get("snippet_highlighted_words", "")
            if isinstance(snippet, list):
                snippet = " ".join(snippet)
                
            date = item.get("date", "")

            # Default source_quality to 50
            results.append({
                "title": title,
                "url": link,
                "snippet": snippet,
                "date": date,
                "source_quality": 50
            })

        return results
