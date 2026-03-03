from pydantic import BaseModel
from typing import List

class SearchResult(BaseModel):
    title: str
    url: str
    snippet: str
    date: str
    source_quality: int

class Finding(BaseModel):
    __contract_name__ = "research"
    __contract_version__ = "0.1.0"

    entity: str
    claim: str
    stance: str
    citations: List[SearchResult]

    @classmethod
    def from_obj(cls, obj: dict) -> "Finding":
        return cls.model_validate(obj)

    def to_obj(self) -> dict:
        return self.model_dump(exclude_none=True)
