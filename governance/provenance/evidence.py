import json
from pathlib import Path
import re
from data_layer.contracts.utils import read_jsonl, sha256_of_file

def redact_text(text: str, patterns: list[str]) -> str:
    if not text or not patterns:
        return text
    for pattern in patterns:
        if pattern == "PAN":
            text = re.sub(r"[A-Z]{5}[0-9]{4}[A-Z]{1}", "[REDACTED]", text)
        elif pattern == "Aadhaar":
            text = re.sub(r"\d{4}\s?\d{4}\s?\d{4}", "[REDACTED]", text)
        elif pattern == "IFSC":
            text = re.sub(r"[A-Z]{4}0[A-Z0-9]{6}", "[REDACTED]", text)
        else:
            text = re.sub(re.escape(pattern), "[REDACTED]", text)
    return text

def build_evidence_pack(job_dir: Path, cfg: dict):
    pack_dir = job_dir / "evidence_pack"
    pack_dir.mkdir(exist_ok=True)
    
    docs_dir = pack_dir / "docs"
    docs_dir.mkdir(exist_ok=True)
    
    web_dir = pack_dir / "web"
    web_dir.mkdir(exist_ok=True)
    
    primary_dir = pack_dir / "primary"
    primary_dir.mkdir(exist_ok=True)
    
    manifest = []
    
    redact_enabled = cfg.get("governance", {}).get("redaction", {}).get("enable_pii_redaction", False)
    pii_patterns = cfg.get("governance", {}).get("redaction", {}).get("pii_patterns", []) if redact_enabled else []
    
    # 1. Primary Insights evidence
    insights_path = job_dir / "primary" / "risk_arguments.jsonl"
    if insights_path.exists():
        insights = read_jsonl(insights_path)
        notes = []
        for i, ins in enumerate(insights):
            quote = ins.get("quote", "")
            if redact_enabled:
                quote = redact_text(quote, pii_patterns)
            notes.append(f"Argument {i+1}:\nQuote: {quote}\nObservation: {ins.get('observation')}\n")
        
        notes_file = primary_dir / "notes.txt"
        with open(notes_file, "w", encoding="utf-8") as f:
            f.write("\n".join(notes))
            
        manifest.append({
            "path": "primary/notes.txt",
            "bytes": notes_file.stat().st_size,
            "sha256": sha256_of_file(notes_file),
            "source_artifact": "risk_arguments.jsonl",
            "contract": "primary"
        })
        
    # 2. Web citations
    research_path = job_dir / "research" / "research_findings.jsonl"
    if research_path.exists():
        findings = read_jsonl(research_path)
        citations = []
        for f in findings:
            for cit in f.get("citations", []):
                snippet = cit.get("snippet", "")
                if redact_enabled:
                    snippet = redact_text(snippet, pii_patterns)
                citations.append({
                    "url": cit.get("url"),
                    "title": cit.get("title", ""),
                    "date": cit.get("date", ""),
                    "snippet": snippet,
                    "when_accessed": ""
                })
                
        web_file = web_dir / "citations.jsonl"
        with open(web_file, "w", encoding="utf-8") as f:
            for c in citations:
                f.write(json.dumps(c) + "\n")
                
        manifest.append({
            "path": "web/citations.jsonl",
            "bytes": web_file.stat().st_size,
            "sha256": sha256_of_file(web_file),
            "source_artifact": "research_findings.jsonl",
            "contract": "research"
        })
        
    # 3. Docs evidence
    facts_path = job_dir / "ingestor" / "facts.jsonl"
    if facts_path.exists():
        facts = read_jsonl(facts_path)
        doc_evidences = []
        for f in facts:
            snippet = f.get("evidence_snippet")
            if snippet:
                if redact_enabled:
                    snippet = redact_text(snippet, pii_patterns)
                doc_evidences.append({
                    "field": f.get("field"),
                    "page": f.get("page"),
                    "snippet": snippet
                })
                
        if doc_evidences:
            doc_file = docs_dir / "extracted_snippets.jsonl"
            with open(doc_file, "w", encoding="utf-8") as f:
                for d in doc_evidences:
                    f.write(json.dumps(d) + "\n")
                    
            manifest.append({
                "path": "docs/extracted_snippets.jsonl",
                "bytes": doc_file.stat().st_size,
                "sha256": sha256_of_file(doc_file),
                "source_artifact": "facts.jsonl",
                "contract": "facts"
            })

    manifest_file = pack_dir / "evidence_manifest.json"
    with open(manifest_file, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
