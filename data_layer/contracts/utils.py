import json
import hashlib
from datetime import datetime, timezone
import re
from pathlib import Path

def write_jsonl(path, items):
    with open(path, "w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item) + "\n")

def read_jsonl(path) -> list[dict]:
    items = []
    if Path(path).exists():
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    items.append(json.loads(line))
    return items

def sha256_of_file(path) -> str:
    if not Path(path).exists():
        return ""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def coerce_currency(s: str) -> tuple[float, str]:
    if not isinstance(s, str):
        try:
            return float(s), "INR"
        except:
            return 0.0, "INR"
    s = s.replace(",", "").strip()
    match = re.search(r"([\d\.]+)", s)
    if not match:
        return 0.0, "INR"
    val = float(match.group(1))
    
    if "Cr" in s or "Crore" in s:
        val *= 10000000
    elif "Lakh" in s:
        val *= 100000
        
    if "(" in s and ")" in s:
        val = -val
    elif "-" in s and s.find("-") < s.find(match.group(1)):
        val = -val

    return val, "INR"

def period_normalize(s: str) -> str:
    # "YYYY" | "YYYY-Qn" | "YYYY-MM"
    if not s:
        return ""
    s = str(s).strip()
    if re.match(r"^\d{4}$", s):
        return s
    m = re.match(r"^(\d{4})[-/]?Q([1-4])$", s, re.IGNORECASE)
    if m:
        return f"{m.group(1)}-Q{m.group(2)}"
    m = re.match(r"^(\d{4})[-/]?(\d{2})$", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return s
