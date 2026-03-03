import logging

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Counter, Histogram, Gauge
    PROM_AVAILABLE = True
    
    # Define metrics
    JOBS_TOTAL = Counter(
        "intellicredit_jobs_total", 
        "Total jobs processed", 
        ["outcome"]
    )
    
    STAGE_DURATION = Histogram(
        "intellicredit_stage_duration_seconds",
        "Duration of each pipeline stage in seconds",
        ["stage"]
    )
    
    VALIDATION_ISSUES = Counter(
        "intellicredit_validation_issues_total",
        "Number of validation issues found",
        ["stage", "severity"]
    )
    
    PROVIDER_CALLS = Counter(
        "intellicredit_provider_calls_total",
        "Total API calls to external providers",
        ["provider", "model"]
    )
    
    TOKENS_TOTAL = Counter(
        "intellicredit_tokens_total",
        "Total tokens used by LLMs",
        ["provider", "model", "type"]
    )
    
    COST_TOTAL = Counter(
        "intellicredit_cost_total",
        "Estimated cost incurred in configured currency",
        ["provider"]
    )
    
except ImportError:
    PROM_AVAILABLE = False
    logger.debug("prometheus_client not available")

def observe_stage_duration(stage: str, duration_sec: float):
    if PROM_AVAILABLE:
        try:
            STAGE_DURATION.labels(stage=stage).observe(duration_sec)
        except Exception:
            pass

def inc_validation_issue(stage: str, severity: str):
    if PROM_AVAILABLE:
        try:
            VALIDATION_ISSUES.labels(stage=stage, severity=severity).inc()
        except Exception:
            pass

def record_provider_usage(usage_dict: dict):
    if PROM_AVAILABLE:
        try:
            provider = usage_dict.get("provider", "unknown")
            model = usage_dict.get("model", "")
            
            PROVIDER_CALLS.labels(provider=provider, model=model).inc(usage_dict.get("calls", 1))
            
            if usage_dict.get("type", "llm") == "llm":
                prompt = usage_dict.get("prompt_tokens", 0)
                completion = usage_dict.get("completion_tokens", 0)
                if prompt > 0:
                    TOKENS_TOTAL.labels(provider=provider, model=model, type="prompt").inc(prompt)
                if completion > 0:
                    TOKENS_TOTAL.labels(provider=provider, model=model, type="completion").inc(completion)
        except Exception:
            pass

def record_cost(provider: str, cost: float):
    if PROM_AVAILABLE and cost > 0:
        try:
            COST_TOTAL.labels(provider=provider).inc(cost)
        except Exception:
            pass

def record_job_outcome(outcome: str):
    if PROM_AVAILABLE:
        try:
            JOBS_TOTAL.labels(outcome=outcome).inc()
        except Exception:
            pass
