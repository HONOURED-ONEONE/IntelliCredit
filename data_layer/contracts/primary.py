from pydantic import BaseModel

class RiskArgument(BaseModel):
    __contract_name__ = "primary"
    __contract_version__ = "0.1.0"

    quote: str
    observation: str
    interpretation: str
    five_c: str
    proposed_delta: int
    freshness_weight: float

    @classmethod
    def from_obj(cls, obj: dict) -> "RiskArgument":
        return cls.model_validate(obj)

    def to_obj(self) -> dict:
        return self.model_dump(exclude_none=True)
