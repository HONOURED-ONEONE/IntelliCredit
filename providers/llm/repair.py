import json
from loguru import logger
from .openai_client import OpenAIClient

def repair_json(raw_text: str, target_schema: dict, api_key: str) -> tuple[dict, dict]:
    """Uses a small model to repair malformed JSON."""
    client = OpenAIClient(api_key=api_key)
    prompt = f"""The following text was supposed to be a JSON object matching this schema: {json.dumps(target_schema)}.
However, it failed to parse. Please fix it and output ONLY valid JSON.

Raw text:
{raw_text}"""
    try:
        return client.complete_json(prompt, target_schema, model="gpt-4o-mini")
    except Exception as e:
        logger.error(f"Failed to repair JSON: {e}")
        raise
