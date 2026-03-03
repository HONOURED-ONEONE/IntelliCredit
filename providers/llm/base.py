from typing import Protocol, List, Dict, Any, Optional

class LLMProvider(Protocol):
    def complete_json(self, prompt: str, schema: dict, model: str, **kwargs) -> tuple[dict, dict]:
        """Returns (parsed_json, usage_metrics)"""
        ...
        
    def vision_extract(self, pages: List[bytes], instructions: str, schema: dict, model: str, **kwargs) -> tuple[List[dict], dict]:
        """Returns (extracted_list, usage_metrics)"""
        ...
