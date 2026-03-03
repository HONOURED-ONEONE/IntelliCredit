from pydantic import BaseModel
from typing import List

class SearchResult(BaseModel):
    title: str
    url: str
    snippet: str
    date: str
    source_quality: int

class Finding(BaseModel):
    entity: str
    claim: str
    stance: str
    citations: List[SearchResult]
