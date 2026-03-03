from typing import Protocol, List

class SearchProvider(Protocol):
    def search(self, query: str) -> List[dict]: ...
