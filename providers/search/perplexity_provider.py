import httpx
from datetime import datetime, timezone
from typing import List
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from data_layer.contracts.research import SearchResult

class PerplexityProvider:
    def __init__(self, api_key: str):
        self.api_key = api_key
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
                
                # Mocking a structured response extraction since Perplexity returns text with markdown links
                # In a real scenario, we'd parse citations. For now, wrap the answer in one synthetic result.
                content = data["choices"][0]["message"]["content"]
                
                res = {
                    "title": f"Perplexity Analysis for: {query}",
                    "url": "https://perplexity.ai/search",
                    "snippet": content[:500],
                    "date": datetime.now(timezone.utc).isoformat(),
                    "source_quality": 90
                }
                return [res]
        except Exception as e:
            logger.error(f"Perplexity search failed: {e}")
            return []
