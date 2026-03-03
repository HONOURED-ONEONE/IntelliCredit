from pydantic import BaseModel
from typing import List

class DecisionOutput(BaseModel):
    decision: str
    limit: float
    rate: float
    drivers: List[str]
