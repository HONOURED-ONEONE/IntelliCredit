import pytest
import json
from unittest.mock import patch, MagicMock
from pathlib import Path
from intelligence.primary.primary_agent import run

def test_primary_model_migration_and_options(tmp_path):
    job_dir = tmp_path / "job_primary"
    cfg = {
        "features": {"enable_live_llm": True},
        "llm": {
            "enable_cache": True,
            "model_map": {"reasoning_primary": "claude-sonnet-4-6"},
            "anthropic": {
                "thinking": {"type": "adaptive"}
            }
        }
    }
    payload = {"notes": "\"Good character\"", "enable_live_llm": True}
    
    with patch("os.getenv", return_value="fake_key"):
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_client_instance = MagicMock()
            mock_anthropic.return_value = mock_client_instance
            
            mock_response = MagicMock()
            mock_response.content = [MagicMock(text='{"arguments": [{"quote": "Good character", "observation": "Good", "interpretation": "Good", "five_c": "Character", "proposed_delta": 5, "note_missing_quote": false}]}')]
            mock_response.usage = MagicMock(input_tokens=10, output_tokens=20)
            
            mock_client_instance.messages.create.return_value = mock_response
            
            run(job_dir, cfg, payload)
            
            mock_client_instance.messages.create.assert_called_once()
            call_kwargs = mock_client_instance.messages.create.call_args[1]
            
            assert call_kwargs["model"] == "claude-sonnet-4-6"
            assert call_kwargs["thinking"] == {"type": "adaptive"}
            
            user_msg = call_kwargs["messages"][0]["content"]
            assert isinstance(user_msg, list)
            assert user_msg[0]["cache_control"] == {"type": "ephemeral"}
            
            args_file = job_dir / "primary" / "risk_arguments.jsonl"
            assert args_file.exists()
            with open(args_file) as f:
                data = json.loads(f.readline())
                assert data["five_c"] == "Character"

def test_primary_thinking_fallback(tmp_path):
    job_dir = tmp_path / "job_primary_fb"
    cfg = {
        "features": {"enable_live_llm": True},
        "llm": {
            "enable_cache": False,
            "model_map": {"reasoning_primary": "claude-sonnet-4-6"},
            "anthropic": {
                "thinking": {"type": "adaptive"}
            }
        }
    }
    payload = {"notes": "\"Good character\"", "enable_live_llm": True}
    
    with patch("os.getenv", return_value="fake_key"):
        with patch("anthropic.Anthropic") as mock_anthropic:
            mock_client_instance = MagicMock()
            mock_anthropic.return_value = mock_client_instance
            
            # First call raises an error about thinking, second succeeds
            mock_response_success = MagicMock()
            mock_response_success.content = [MagicMock(text='{"arguments": [{"quote": "Good character", "observation": "Good", "interpretation": "Good", "five_c": "Character", "proposed_delta": 5, "note_missing_quote": false}]}')]
            mock_response_success.usage = MagicMock(input_tokens=10, output_tokens=20)
            
            mock_client_instance.messages.create.side_effect = [
                Exception("thinking is not supported"),
                mock_response_success
            ]
            
            run(job_dir, cfg, payload)
            
            assert mock_client_instance.messages.create.call_count == 2
            
            call_1_kwargs = mock_client_instance.messages.create.call_args_list[0][1]
            assert "thinking" in call_1_kwargs
            
            call_2_kwargs = mock_client_instance.messages.create.call_args_list[1][1]
            assert "thinking" not in call_2_kwargs
            
            user_msg = call_2_kwargs["messages"][0]["content"]
            assert "cache_control" not in user_msg[0]
