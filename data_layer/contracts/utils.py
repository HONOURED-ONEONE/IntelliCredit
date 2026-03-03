import json
from pathlib import Path
from typing import Iterable

def write_jsonl(path: Path, items: Iterable[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item) + "
")
