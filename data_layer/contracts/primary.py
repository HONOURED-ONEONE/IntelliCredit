from pydantic import BaseModel

class RiskArgument(BaseModel):
    quote: str
    observation: str
    interpretation: str
    five_c: str
    proposed_delta: int
    freshness_weight: float
