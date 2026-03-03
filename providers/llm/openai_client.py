import json
import base64
import httpx
from typing import List, Dict, Any
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from .base import LLMProvider

class OpenAIClient(LLMProvider):
    def __init__(self, api_key: str):
        from openai import OpenAI
        self.client = OpenAI(api_key=api_key)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def complete_json(self, prompt: str, schema: dict, model: str, **kwargs) -> tuple[dict, dict]:
        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                max_tokens=kwargs.get("max_tokens", 3000),
                timeout=kwargs.get("timeout", 60)
            )
            content = response.choices[0].message.content
            usage = response.usage
            metrics = {
                "provider": "openai",
                "prompt_tokens": usage.prompt_tokens if usage else 0,
                "completion_tokens": usage.completion_tokens if usage else 0,
                "model": model
            }
            return json.loads(content), metrics
        except Exception as e:
            logger.error(f"OpenAI complete_json failed: {e}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def vision_extract(self, pages: List[bytes], instructions: str, schema: dict, model: str, **kwargs) -> tuple[List[dict], dict]:
        messages = [{"role": "system", "content": "You are a data extraction assistant. Output JSON strictly matching the requested schema."}]
        content_items = [{"type": "text", "text": instructions}]
        
        for img_bytes in pages:
            b64 = base64.b64encode(img_bytes).decode('utf-8')
            content_items.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{b64}"}
            })
            
        messages.append({"role": "user", "content": content_items})
        
        # Add schema hint to instructions
        messages[0]["content"] += f"
Schema: {json.dumps(schema)}"

        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=messages,
                response_format={"type": "json_object"},
                max_tokens=kwargs.get("max_tokens", 2000),
                timeout=kwargs.get("timeout", 60)
            )
            content = response.choices[0].message.content
            usage = response.usage
            metrics = {
                "provider": "openai",
                "prompt_tokens": usage.prompt_tokens if usage else 0,
                "completion_tokens": usage.completion_tokens if usage else 0,
                "model": model
            }
            res_json = json.loads(content)
            # Depending on schema, it might be wrapped in a root key. 
            # We assume the schema requests a list or an object containing a list.
            if isinstance(res_json, dict) and len(res_json.keys()) == 1:
                key = list(res_json.keys())[0]
                if isinstance(res_json[key], list):
                    return res_json[key], metrics
            return [res_json] if isinstance(res_json, dict) else res_json, metrics
        except Exception as e:
            logger.error(f"OpenAI vision_extract failed: {e}")
            raise
