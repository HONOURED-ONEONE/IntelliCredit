import os
import httpx
from datetime import datetime, timezone
from typing import List
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

class PerplexityProvider:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.api_key = os.getenv("PPLX_API_KEY")
        self.url = "https://api.perplexity.ai/chat/completions"

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=5))
    def search(self, query: str, freshness_days: int) -> List[dict]:
        if not self.api_key:
            logger.warning("No PPLX_API_KEY provided, returning empty results")
            return []
            
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        prompt = f"Search for recent news and facts regarding: {query}. Focus on adverse media, litigation, or significant financial events. Provide citations."
        
        payload = {
            "model": "sonar-pro",
            "messages": [{"role": "user", "content": prompt}]
        }
        
        try:
            with httpx.Client(timeout=30.0) as client:
                resp = client.post(self.url, headers=headers, json=payload)
                resp.raise_for_status()
                data = resp.json()
                
                content = data["choices"][0]["message"]["content"]
                citations = data.get("citations", [])
                
                results = []
                for i, url in enumerate(citations):
                    results.append({
                        "title": f"Source {i+1} for: {query}",
                        "url": url,
                        "snippet": content[:300] + "...",
                        "date": datetime.now(timezone.utc).isoformat(),
                        "source_quality": 0 # will be calculated in agent
                    })
                
                if not results:
                    import re
                    urls = re.findall(r'(https?://[^\s\)]+)', content)
                    urls = list(dict.fromkeys(urls))
                    for i, url in enumerate(urls[:5]):
                        results.append({
                            "title": f"Extracted Source {i+1}",
                            "url": url,
                            "snippet": content[:300] + "...",
                            "date": datetime.now(timezone.utc).isoformat(),
                            "source_quality": 0
                        })
                        
                if not results:
                    results.append({
                        "title": f"Perplexity Analysis for: {query}",
                        "url": "https://perplexity.ai/search",
                        "snippet": content[:500],
                        "date": datetime.now(timezone.utc).isoformat(),
                        "source_quality": 0
                    })
                return results
        except Exception as e:
            if self.cfg.get("security", {}).get("redact_keys_in_logs", True):
                logger.error("Perplexity search failed")
            else:
                logger.error(f"Perplexity search failed: {e}")
            return []
