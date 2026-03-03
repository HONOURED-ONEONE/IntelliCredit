import json
from typing import List, Dict, Any
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from .base import LLMProvider

class AnthropicClient(LLMProvider):
    def __init__(self, api_key: str):
        from anthropic import Anthropic
        self.client = Anthropic(api_key=api_key)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def complete_json(self, prompt: str, schema: dict, model: str, **kwargs) -> tuple[dict, dict]:
        sys_prompt = "You are a helpful assistant. Please output strictly valid JSON conforming to the requested schema. No markdown wrapping."
        full_prompt = f"{prompt}

Schema:
{json.dumps(schema)}"
        try:
            response = self.client.messages.create(
                model=model,
                max_tokens=kwargs.get("max_tokens", 3000),
                system=sys_prompt,
                messages=[{"role": "user", "content": full_prompt}]
            )
            content = response.content[0].text
            usage = response.usage
            metrics = {
                "provider": "anthropic",
                "prompt_tokens": usage.input_tokens,
                "completion_tokens": usage.output_tokens,
                "model": model
            }
            # Attempt to parse json, stripping markdown if present
            content_stripped = content.strip()
            if content_stripped.startswith("```json"):
                content_stripped = content_stripped.split("```json")[1].rsplit("```", 1)[0].strip()
            return json.loads(content_stripped), metrics
        except Exception as e:
            logger.error(f"Anthropic complete_json failed: {e}")
            raise

    def vision_extract(self, pages: List[bytes], instructions: str, schema: dict, model: str, **kwargs) -> tuple[List[dict], dict]:
        raise NotImplementedError("Vision extraction not currently used via Anthropic in this demo phase.")
