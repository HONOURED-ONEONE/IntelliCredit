import pytest
import base64
import json
from unittest.mock import patch, MagicMock
from providers.llm.openai_client import OpenAIClient

def test_openai_vision_payload():
    with patch("openai.OpenAI") as mock_openai:
        mock_client_instance = MagicMock()
        mock_openai.return_value = mock_client_instance
        
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content='{"fact": "value"}'))]
        mock_response.usage = MagicMock(prompt_tokens=10, completion_tokens=5)
        
        mock_client_instance.chat.completions.create.return_value = mock_response
        
        client = OpenAIClient(api_key="fake")
        
        pages = [b"fake_image_bytes"]
        instructions = "Extract data"
        schema = {"type": "object"}
        model = "gpt-4o"
        
        result, metrics = client.vision_extract(pages, instructions, schema, model, detail="low")
        
        mock_client_instance.chat.completions.create.assert_called_once()
        call_kwargs = mock_client_instance.chat.completions.create.call_args[1]
        
        assert call_kwargs["model"] == "gpt-4o"
        assert call_kwargs["response_format"] == {"type": "json_object"}
        
        messages = call_kwargs["messages"]
        user_msg = messages[1]
        assert user_msg["role"] == "user"
        content_items = user_msg["content"]
        
        assert content_items[0]["type"] == "text"
        assert content_items[1]["type"] == "image_url"
        
        expected_b64 = base64.b64encode(b"fake_image_bytes").decode("utf-8")
        assert content_items[1]["image_url"]["url"] == f"data:image/jpeg;base64,{expected_b64}"
        assert content_items[1]["image_url"]["detail"] == "low"

def test_openai_vision_invalid_json_retry():
    with patch("openai.OpenAI") as mock_openai:
        mock_client_instance = MagicMock()
        mock_openai.return_value = mock_client_instance
        
        bad_response = MagicMock()
        bad_response.choices = [MagicMock(message=MagicMock(content='I am not JSON'))]
        
        good_response = MagicMock()
        good_response.choices = [MagicMock(message=MagicMock(content='{"fixed": "yes"}'))]
        good_response.usage = MagicMock(prompt_tokens=10, completion_tokens=5)
        
        mock_client_instance.chat.completions.create.side_effect = [bad_response, good_response]
        
        client = OpenAIClient(api_key="fake")
        
        result, metrics = client.vision_extract([b"image"], "Extract", {}, "gpt-4o")
        
        assert mock_client_instance.chat.completions.create.call_count == 2
        
        # Check that the second call has the stronger instructions appended
        call_2_kwargs = mock_client_instance.chat.completions.create.call_args_list[1][1]
        messages_2 = call_2_kwargs["messages"]
        assert "Return ONLY valid JSON object" in messages_2[0]["content"]
        
        assert result == [{"fixed": "yes"}]
