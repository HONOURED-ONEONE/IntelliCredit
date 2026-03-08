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
        full_prompt = f"{prompt}\n\nSchema:\n{json.dumps(schema)}"
        
        user_content = [{"type": "text", "text": full_prompt}]
        enable_cache = kwargs.get("enable_cache", True)
        if enable_cache:
            user_content[0]["cache_control"] = {"type": "ephemeral"}
            
        create_kwargs = {
            "model": model,
            "max_tokens": kwargs.get("max_tokens", 3000),
            "system": sys_prompt,
            "messages": [{"role": "user", "content": user_content}]
        }
        
        thinking_cfg = kwargs.get("thinking")
        if thinking_cfg and thinking_cfg.get("type") != "none":
            create_kwargs["thinking"] = thinking_cfg
            
        try:
            try:
                response = self.client.messages.create(**create_kwargs)
            except Exception as e:
                if "thinking" in create_kwargs and ("thinking" in str(e).lower() or "unsupported" in str(e).lower() or "invalid" in str(e).lower()):
                    logger.warning(f"Thinking parameter rejected, retrying without: {e}")
                    del create_kwargs["thinking"]
                    response = self.client.messages.create(**create_kwargs)
                else:
                    raise
                    
            content = response.content[0].text if not getattr(response.content[0], "type", "") == "thinking" else response.content[-1].text
            usage = response.usage
            metrics = {
                "provider": "anthropic",
                "prompt_tokens": getattr(usage, "input_tokens", 0),
                "completion_tokens": getattr(usage, "output_tokens", 0),
                "model": model,
                "thinking": create_kwargs.get("thinking", {}).get("type", "none"),
                "cache": "ephemeral" if enable_cache else "none"
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
