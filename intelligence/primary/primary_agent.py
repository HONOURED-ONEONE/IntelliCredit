import json
import re
import os
from pathlib import Path
from loguru import logger
from governance.validation.validators import write_validation_report
from governance.provenance.provenance import append_metrics

def run(job_dir: Path, cfg: dict, payload: dict) -> None:
    out_dir = job_dir / "primary"
    out_dir.mkdir(parents=True, exist_ok=True)

    notes = payload.get("notes", "")
    arguments = []
    missing_quote = False
    enable_live_llm = payload.get("enable_live_llm", cfg.get("features", {}).get("enable_live_llm", False))

    if enable_live_llm and os.getenv("ANTHROPIC_API_KEY"):
        try:
            from providers.llm.anthropic_client import AnthropicClient
            from providers.llm.repair import repair_json
            
            client = AnthropicClient(api_key=os.getenv("ANTHROPIC_API_KEY"))
            reasoning_model = payload.get("reasoning_model") or cfg.get("llm", {}).get("model_map", {}).get("reasoning_primary", "claude-3-7-sonnet-2025-10-22")
            
            schema = {
                "type": "object",
                "properties": {
                    "arguments": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "quote": {"type": "string"},
                                "observation": {"type": "string"},
                                "interpretation": {"type": "string"},
                                "five_c": {"type": "string", "enum": ["Character", "Capacity", "Capital", "Conditions", "Collateral"]},
                                "proposed_delta": {"type": "integer", "minimum": -10, "maximum": 10},
                                "freshness_weight": {"type": "number"},
                                "note_missing_quote": {"type": "boolean"},
                                "contradicts": {"type": "array", "items": {"type": "string"}}
                            },
                            "required": ["quote", "observation", "interpretation", "five_c", "proposed_delta", "note_missing_quote"]
                        }
                    }
                },
                "required": ["arguments"]
            }
            
            prompt = f"Analyze these officer notes and extract risk arguments strictly based on explicit quotes. If there's no quote in the notes, note_missing_quote must be true and propose 0 delta. Match the observation to one of the 5Cs.\n\nNotes:\n{notes}"
            
            parsed, metrics = client.complete_json(prompt, schema, model=reasoning_model)
            append_metrics(job_dir, "primary_reasoning", metrics)
            
            # Simple check: if arguments list is missing, we might need repair
            if "arguments" not in parsed:
                logger.warning("Missing arguments key, attempting repair")
                parsed, rep_metrics = repair_json(json.dumps(parsed), schema, os.getenv("OPENAI_API_KEY"))
                append_metrics(job_dir, "primary_repair", rep_metrics)
                
            arguments = parsed.get("arguments", [])
            
            # Post-process to enforce "no quote -> no argument" conceptually
            for arg in arguments:
                if arg.get("note_missing_quote"):
                    arg["proposed_delta"] = 0
                
        except Exception as e:
            logger.error(f"LLM primary extraction failed: {e}. Falling back to mock logic.")
            arguments = []

    if not arguments:
        match = re.search(r'"([^"]*)"', notes)
        if match:
            quote = match.group(1)
            obs = "Quoted observation found"
        else:
            quote = "No explicit quote provided"
            obs = "Inferred from notes"
            missing_quote = True

        five_c = "Capacity"
        delta = 0
        if "good" in notes.lower(): delta = 5
        if "bad" in notes.lower(): delta = -5

        if notes.strip():
            arguments.append({
                "quote": quote,
                "observation": obs,
                "interpretation": "Analyzed from officer notes",
                "five_c": five_c,
                "proposed_delta": delta,
                "freshness_weight": 1.0,
                "note_missing_quote": missing_quote
            })

    with open(out_dir / "risk_arguments.jsonl", "w") as f:
        for arg in arguments:
            f.write(json.dumps(arg) + "\n")

    # A1. Contradiction Heuristics
    contradictions = {"pairs": [], "notes": "Detected via polarity & semantic similarity heuristics."}
    for i in range(len(arguments)):
        for j in range(i + 1, len(arguments)):
            arg1, arg2 = arguments[i], arguments[j]
            if arg1.get("five_c") == arg2.get("five_c"):
                delta1 = arg1.get("proposed_delta", 0)
                delta2 = arg2.get("proposed_delta", 0)
                if (delta1 * delta2 < 0) or abs(delta1 - delta2) >= 5:
                    words1 = set(str(arg1.get("observation", "")).lower().split())
                    words2 = set(str(arg2.get("observation", "")).lower().split())
                    if words1 and words2:
                        overlap = len(words1.intersection(words2)) / len(words1.union(words2))
                        if overlap > 0.2:
                            contradictions["pairs"].append([i, j])

    if contradictions["pairs"]:
        with open(out_dir / "contradictions.json", "w") as f:
            json.dump(contradictions, f, indent=2)

    # A2. Freshness Decay on Deltas
    import datetime
    weight = 1.0
    recency_params = None
    visit_date_str = payload.get("visit_date")
    if visit_date_str:
        try:
            visit_date = datetime.datetime.fromisoformat(visit_date_str.replace("Z", "+00:00"))
            if visit_date.tzinfo is None:
                visit_date = visit_date.replace(tzinfo=datetime.timezone.utc)
            now = datetime.datetime.now(datetime.timezone.utc)
            days = (now - visit_date).days
            if days > 0:
                weight = max(0.3, min(1.0, 0.5 ** (days / 90.0)))
                recency_params = {"half_life_days": 90, "days_since_visit": days, "weight": weight}
        except Exception as e:
            logger.warning(f"Error parsing visit_date: {e}")

    if recency_params:
        with open(out_dir / "weights.json", "w") as f:
            json.dump({"weight": weight, "params": recency_params}, f, indent=2)

    weighted_total_delta = 0
    for arg in arguments:
        weighted_total_delta += round(arg.get("proposed_delta", 0) * weight)

    # A3. Quote -> Document Evidence Link
    doc_path = job_dir / "ingestor" / "documents.jsonl"
    documents = []
    if doc_path.exists():
        with open(doc_path, "r") as f:
            for line in f:
                if line.strip():
                    documents.append(json.loads(line))

    quote_links = []
    for idx, arg in enumerate(arguments):
        quote = arg.get("quote", "")
        if not quote or arg.get("note_missing_quote"):
            continue
            
        best_match = None
        best_score = 0
        quote_lower = quote.lower()
        words_q = set(quote_lower.split())
        
        for doc in documents:
            for p in doc.get("pages", []):
                text = str(p.get("text", "")).lower()
                if quote_lower in text:
                    best_match = {"arg_index": idx, "file": doc.get("file"), "page": p.get("page"), "snippet": text[:300]}
                    best_score = 1.0
                    break
                else:
                    words_t = set(text.split())
                    if words_q:
                        score = len(words_q.intersection(words_t)) / len(words_q)
                        if score > best_score and score > 0.3:
                            best_score = score
                            best_match = {"arg_index": idx, "file": doc.get("file"), "page": p.get("page"), "snippet": p.get("text", "")[:300]}
            if best_score == 1.0:
                break
                
        if best_match:
            quote_links.append(best_match)

    if quote_links:
        with open(out_dir / "quote_links.jsonl", "w") as f:
            for ql in quote_links:
                f.write(json.dumps(ql) + "\n")

    impact_report = {
        "arguments_count": len(arguments),
        "missing_quote_flags": sum(1 for a in arguments if a.get("note_missing_quote", False)),
        "total_delta": sum(a.get("proposed_delta", 0) for a in arguments),
        "weighted_total_delta": weighted_total_delta
    }
    
    if recency_params:
        impact_report["recency_params"] = recency_params

    with open(out_dir / "impact_report.json", "w") as f:
        json.dump(impact_report, f, indent=2)

    write_validation_report("primary", out_dir, {"arguments_count": len(arguments)})
