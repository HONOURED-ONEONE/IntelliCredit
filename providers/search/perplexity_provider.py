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
                
                import re
                md_links = re.findall(r'\[([^\]]+)\]\((https?://[^\)]+)\)', content)
                plain_urls = re.findall(r'(?<!\()(https?://[^\s\)]+)', content)
                
                seen_urls = set()
                results = []
                
                def get_quality(u):
                    u_lower = u.lower()
                    if '.gov.in' in u_lower or 'rbi.org.in' in u_lower or 'reuters.com' in u_lower:
                        return 80
                    return 60
                    
                for title, url in md_links:
                    if url not in seen_urls and len(results) < 5:
                        seen_urls.add(url)
                        results.append({
                            "title": title,
                            "url": url,
                            "snippet": content[:300] + "...",
                            "date": datetime.now(timezone.utc).isoformat(),
                            "source_quality": get_quality(url)
                        })
                        
                for url in plain_urls:
                    if url not in seen_urls and len(results) < 5:
                        seen_urls.add(url)
                        results.append({
                            "title": f"Extracted Source for: {query}",
                            "url": url,
                            "snippet": content[:300] + "...",
                            "date": datetime.now(timezone.utc).isoformat(),
                            "source_quality": get_quality(url)
                        })
                        
                for url in citations:
                    if url not in seen_urls and len(results) < 5:
                        seen_urls.add(url)
                        results.append({
                            "title": f"Citation for: {query}",
                            "url": url,
                            "snippet": content[:300] + "...",
                            "date": datetime.now(timezone.utc).isoformat(),
                            "source_quality": get_quality(url)
                        })
                        
                if not results:
                    results.append({
                        "title": f"Perplexity Analysis for: {query}",
                        "url": "https://perplexity.ai/search",
                        "snippet": content[:500],
                        "date": datetime.now(timezone.utc).isoformat(),
                        "source_quality": 60
                    })
                return results
        except Exception as e:
            if self.cfg.get("security", {}).get("redact_keys_in_logs", True):
                logger.error("Perplexity search failed")
            else:
                logger.error(f"Perplexity search failed: {e}")
            return []
