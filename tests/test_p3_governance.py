import pytest
import json
import tempfile
from pathlib import Path
from governance.validation.aggregate import aggregate_reports
from governance.provenance.metrics_append import append_usage

def test_validation_aggregate():
    with tempfile.TemporaryDirectory() as td:
        job_dir = Path(td)
        
        # Write dummy reports
        r1 = {
            "schema_ok": True,
            "summary": {"ok": 5, "warn": 1, "critical": 0},
            "issues": [
                {"stage": "ingestor", "severity": "WARN", "code": "TEST", "message": "Test issue"}
            ]
        }
        with open(job_dir / "ingestor_validation_report.json", "w") as f:
            json.dump(r1, f)
            
        r2 = {
            "schema_ok": False,
            "summary": {"ok": 2, "warn": 0, "critical": 1},
            "issues": [
                {"stage": "decision", "severity": "CRITICAL", "code": "FAIL", "message": "Failed"}
            ]
        }
        with open(job_dir / "decision_validation_report.json", "w") as f:
            json.dump(r2, f)
            
        agg = aggregate_reports(job_dir)
        
        assert agg["summary"]["ok"] == 7
        assert agg["summary"]["warn"] == 1
        assert agg["summary"]["critical"] == 1
        assert len(agg["issues"]) == 2
        
        assert (job_dir / "validation_aggregate.json").exists()

def test_metrics_append():
    with tempfile.TemporaryDirectory() as td:
        job_dir = Path(td)
        
        config = {
            "billing": {
                "currency": "USD",
                "openai": {
                    "gpt-4o": {
                        "prompt_per_1k": 0.01,
                        "completion_per_1k": 0.03
                    }
                },
                "search": {
                    "perplexity_per_call": 0.05
                }
            }
        }
        
        # Test LLM Usage
        llm_usage = {
            "type": "llm",
            "provider": "openai",
            "model": "gpt-4o",
            "prompt_tokens": 1000,
            "completion_tokens": 1000,
            "calls": 1
        }
        
        append_usage(job_dir, config, llm_usage)
        
        with open(job_dir / "metrics.json", "r") as f:
            m = json.load(f)
            
        assert m["usage"]["llm"]["openai:gpt-4o"]["prompt_tokens"] == 1000
        assert m["usage"]["llm"]["openai:gpt-4o"]["completion_tokens"] == 1000
        assert m["usage"]["llm"]["openai:gpt-4o"]["calls"] == 1
        
        cost = m["cost_estimate"]["total"]
        # 1000 prompt = 0.01, 1000 comp = 0.03, total = 0.04
        assert abs(cost - 0.04) < 0.0001
        
        # Test Search Usage
        search_usage = {
            "type": "search",
            "provider": "perplexity",
            "calls": 2
        }
        
        append_usage(job_dir, config, search_usage)
        
        with open(job_dir / "metrics.json", "r") as f:
            m = json.load(f)
            
        assert m["usage"]["search"]["perplexity"]["calls"] == 2
        cost = m["cost_estimate"]["total"]
        # 2 calls * 0.05 = 0.1 + previous 0.04 = 0.14
        assert abs(cost - 0.14) < 0.0001
