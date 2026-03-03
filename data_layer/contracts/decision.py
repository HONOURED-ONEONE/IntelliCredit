from pydantic import BaseModel
from typing import List

class DecisionOutput(BaseModel):
    __contract_name__ = "decision"
    __contract_version__ = "0.1.0"

    decision: str
    limit: float
    rate: float
    drivers: List[str]

    @classmethod
    def from_obj(cls, obj: dict) -> "DecisionOutput":
        return cls.model_validate(obj)

    def to_obj(self) -> dict:
        return self.model_dump(exclude_none=True)
