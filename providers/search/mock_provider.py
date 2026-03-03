import json
from pathlib import Path
from typing import List
from .base import SearchProvider

class MockSearchProvider(SearchProvider):
    def __init__(self):
        self.dataset_path = Path(__file__).resolve().parent.parent / "mock" / "data" / "research_dataset.jsonl"
        self.data = []
        if self.dataset_path.exists():
            with open(self.dataset_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        self.data.append(json.loads(line))

    def search(self, query: str) -> List[dict]:
        results = []
        query_lower = query.lower()
        seen_urls = set()
        for item in self.data:
            text_to_search = f"{item.get('title','')} {item.get('snippet','')} {item.get('url','')}".lower()
            if query_lower in text_to_search or not query_lower:
                url = item.get("url")
                if url not in seen_urls:
                    seen_urls.add(url)
                    # Simple source quality heuristic
                    sq = 80 if "gov" in url or "reuters" in url else 50
                    item["source_quality"] = sq
                    results.append(item)
        return results
