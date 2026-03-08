import pytest
from providers.search.ensemble_provider import EnsembleSearchProvider
from providers.search.mock_provider import MockSearchProvider
from providers.search.base import SearchProvider

class FakeProvider(SearchProvider):
    def __init__(self, results):
        self.results = results
    def search(self, query: str, top_k: int = 5, **kwargs) -> list:
        return self.results

def test_search_fusion_consensus():
    # A appears in all 3 at rank 5, 5, 5. (rrf_score = 3 * (1/65) = 0.046)
    # B appears only in one at rank 1. (rrf_score = 1 * (1/61) = 0.016)
    
    p1 = FakeProvider([
        {"title": "B", "url": "http://b.com", "snippet": "b"},
        {"title": "X", "url": "http://x.com", "snippet": "x"},
        {"title": "Y", "url": "http://y.com", "snippet": "y"},
        {"title": "Z", "url": "http://z.com", "snippet": "z"},
        {"title": "A", "url": "http://a.com", "snippet": "a"},
    ])
    p2 = FakeProvider([
        {"title": "C", "url": "http://c.com", "snippet": "c"},
        {"title": "D", "url": "http://d.com", "snippet": "d"},
        {"title": "E", "url": "http://e.com", "snippet": "e"},
        {"title": "F", "url": "http://f.com", "snippet": "f"},
        {"title": "A", "url": "http://a.com", "snippet": "a"},
    ])
    p3 = FakeProvider([
        {"title": "G", "url": "http://g.com", "snippet": "g"},
        {"title": "H", "url": "http://h.com", "snippet": "h"},
        {"title": "I", "url": "http://i.com", "snippet": "i"},
        {"title": "J", "url": "http://j.com", "snippet": "j"},
        {"title": "A", "url": "http://a.com", "snippet": "a"},
    ])
    
    ensemble = EnsembleSearchProvider({"p1": p1, "p2": p2, "p3": p3}, {})
    
    results = ensemble.search("query")
    
    # A should outrank B
    urls = [r["url"] for r in results]
    assert urls[0] == "http://a.com"
    assert "http://b.com" in urls

def test_search_fusion_dedup():
    p1 = FakeProvider([
        {"title": "A", "url": "http://a.com?utm_source=1", "snippet": "a1"},
    ])
    p2 = FakeProvider([
        {"title": "A2", "url": "http://a.com?utm_medium=2", "snippet": "a2 longer"},
    ])
    p3 = FakeProvider([
        {"title": "A3", "url": "http://a.com", "snippet": "a"},
    ])
    
    ensemble = EnsembleSearchProvider({"p1": p1, "p2": p2, "p3": p3}, {})
    results = ensemble.search("query")
    
    assert len(results) == 1
    assert len(results[0]["providers_hit"]) == 3
    assert results[0]["snippet"] == "a2 longer"
    assert results[0]["title"] in ["A", "A2", "A3"]
