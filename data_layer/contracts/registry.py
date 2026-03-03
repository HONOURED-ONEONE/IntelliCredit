from pathlib import Path
import json
from .facts import Fact
from .research import SearchResult, Finding
from .primary import RiskArgument
from .decision import DecisionOutput

# A simple registry mapping name to Pydantic model
_REGISTRY = {
    "facts": Fact,
    "research": Finding,
    "primary": RiskArgument,
    "decision": DecisionOutput,
}

def get_model(name: str):
    return _REGISTRY.get(name)

def json_schema(name: str) -> dict:
    model = get_model(name)
    if not model:
        return {}
    return model.model_json_schema()

def validate_artifact(path: Path, name: str) -> dict:
    """Returns {ok: bool, errors: [...], counts: {...}}"""
    model = get_model(name)
    if not model:
        return {"ok": False, "errors": [f"Unknown contract: {name}"], "counts": {}}
    
    if not path.exists():
        return {"ok": False, "errors": [f"File not found: {path}"], "counts": {}}
    
    errors = []
    records = 0
    
    if path.suffix == ".jsonl":
        with open(path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if not line.strip():
                    continue
                records += 1
                try:
                    obj = json.loads(line)
                    model.model_validate(obj)
                except Exception as e:
                    errors.append(f"Line {i+1}: {e}")
    elif path.suffix == ".json":
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            model.model_validate(obj)
            records = 1
        except Exception as e:
            errors.append(f"JSON object: {e}")
    else:
        errors.append(f"Unsupported extension: {path.suffix}")

    return {
        "ok": len(errors) == 0,
        "errors": errors,
        "counts": {"records": records}
    }

def export_all_schemas(out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    for name in _REGISTRY:
        schema = json_schema(name)
        with open(out_dir / f"{name}.schema.json", "w") as f:
            json.dump(schema, f, indent=2)
