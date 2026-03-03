import json
from pathlib import Path

def append_usage(job_dir: Path, config: dict, usage_dict: dict):
    """
    usage_dict format for LLM:
    {
        "type": "llm",
        "provider": "openai",
        "model": "gpt-4o",
        "prompt_tokens": 100,
        "completion_tokens": 50,
        "calls": 1
    }
    
    usage_dict format for Search:
    {
        "type": "search",
        "provider": "perplexity",
        "calls": 1
    }
    """
    metrics_file = job_dir / "metrics.json"
    data = {}
    if metrics_file.exists():
        try:
            with open(metrics_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            pass
            
    if "usage" not in data:
        data["usage"] = {"llm": {}, "search": {}}
    if "cost_estimate" not in data:
        curr = config.get("billing", {}).get("currency", "USD")
        data["cost_estimate"] = {"total": 0.0, "breakdown": {}, "currency": curr}
        
    usage_type = usage_dict.get("type", "llm")
    provider = usage_dict.get("provider", "unknown")
    model = usage_dict.get("model", "")
    
    billing_cfg = config.get("billing", {})
    cost = 0.0
    
    try:
        from governance.observability.prom import record_provider_usage
        record_provider_usage(usage_dict)
    except Exception:
        pass
    
    if usage_type == "llm":
        key = f"{provider}:{model}" if model else provider
        if key not in data["usage"]["llm"]:
            data["usage"]["llm"][key] = {"prompt_tokens": 0, "completion_tokens": 0, "calls": 0}
            
        data["usage"]["llm"][key]["prompt_tokens"] += usage_dict.get("prompt_tokens", 0)
        data["usage"]["llm"][key]["completion_tokens"] += usage_dict.get("completion_tokens", 0)
        data["usage"]["llm"][key]["calls"] += usage_dict.get("calls", 1)
        
        provider_billing = billing_cfg.get(provider, {})
        model_billing = provider_billing.get(model, {})
        p_cost = (usage_dict.get("prompt_tokens", 0) / 1000.0) * model_billing.get("prompt_per_1k", 0.0)
        c_cost = (usage_dict.get("completion_tokens", 0) / 1000.0) * model_billing.get("completion_per_1k", 0.0)
        cost = p_cost + c_cost
        
    elif usage_type == "search":
        key = provider
        if key not in data["usage"]["search"]:
            data["usage"]["search"][key] = {"calls": 0}
            
        data["usage"]["search"][key]["calls"] += usage_dict.get("calls", 1)
        
        search_billing = billing_cfg.get("search", {})
        cost_per_call = search_billing.get(f"{provider}_per_call", 0.0)
        cost = usage_dict.get("calls", 1) * cost_per_call

    data["cost_estimate"]["total"] += cost
    if key not in data["cost_estimate"]["breakdown"]:
        data["cost_estimate"]["breakdown"][key] = 0.0
    data["cost_estimate"]["breakdown"][key] += cost

    try:
        from governance.observability.prom import record_cost
        record_cost(provider, cost)
    except Exception:
        pass

    with open(metrics_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
