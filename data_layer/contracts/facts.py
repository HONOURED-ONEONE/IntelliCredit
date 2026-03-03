from pydantic import BaseModel
from typing import Optional, Union

class Fact(BaseModel):
    field: str
    value: Union[str, float, int]
    unit: Optional[str] = None
    period: Optional[str] = None
    page: Optional[int] = None
    evidence_snippet: Optional[str] = None
    confidence: Optional[float] = None
