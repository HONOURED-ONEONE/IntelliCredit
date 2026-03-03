import urllib.parse
from datetime import datetime, timezone

class IndianKanoonProvider:
    def __init__(self, cfg: dict):
        self.enabled = cfg.get("search", {}).get("legal_sources", {}).get("indiankanoon", {}).get("enabled", False)
        
    def search(self, query: str) -> list[dict]:
        """Stub returning up to 2 synthetic citations per query if enabled."""
        if not self.enabled:
            return []
            
        citations = []
        now_iso = datetime.now(timezone.utc).isoformat()
        
        encoded_query = urllib.parse.quote(query)
        
        citations.append({
            "title": f"Indian Kanoon Search: {query}",
            "url": f"https://indiankanoon.org/search/?formInput={encoded_query}",
            "snippet": "Potential legal reference (manual review)",
            "date": now_iso,
            "source_quality": 85
        })
        
        return citations
