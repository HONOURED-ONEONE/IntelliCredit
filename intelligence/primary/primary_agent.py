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

    impact_report = {
        "arguments_count": len(arguments),
        "missing_quote_flags": sum(1 for a in arguments if a.get("note_missing_quote", False)),
        "total_delta": sum(a.get("proposed_delta", 0) for a in arguments)
    }

    with open(out_dir / "impact_report.json", "w") as f:
        json.dump(impact_report, f, indent=2)

    write_validation_report("primary", out_dir, {"arguments_count": len(arguments)})
