from pydantic import BaseModel
from typing import Optional, Union

class Fact(BaseModel):
    __contract_name__ = "facts"
    __contract_version__ = "0.1.0"
    
    field: str
    value: Union[str, float, int]
    unit: Optional[str] = None
    period: Optional[str] = None
    page: Optional[int] = None
    evidence_snippet: Optional[str] = None
    confidence: Optional[float] = None
    
    @classmethod
    def from_obj(cls, obj: dict) -> "Fact":
        return cls.model_validate(obj)

    def to_obj(self) -> dict:
        return self.model_dump(exclude_none=True)
