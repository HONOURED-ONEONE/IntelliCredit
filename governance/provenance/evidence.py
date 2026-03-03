import json
from pathlib import Path
import re
from data_layer.contracts.utils import read_jsonl, sha256_of_file

def redact_text(text: str, patterns: list[str], redaction_stats: dict = None) -> str:
    if not text or not patterns:
        return text
    
    initial_redactions = text.count("[REDACTED]")
    for pattern in patterns:
        if pattern == "PAN":
            text = re.sub(r"[A-Z]{5}[0-9]{4}[A-Z]{1}", "[REDACTED]", text)
        elif pattern == "Aadhaar":
            text = re.sub(r"\d{4}\s?\d{4}\s?\d{4}", "[REDACTED]", text)
        elif pattern == "IFSC":
            text = re.sub(r"[A-Z]{4}0[A-Z0-9]{6}", "[REDACTED]", text)
        else:
            text = re.sub(re.escape(pattern), "[REDACTED]", text)
            
    if redaction_stats is not None:
        redaction_stats["count"] += text.count("[REDACTED]") - initial_redactions
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
    store_page_images = cfg.get("governance", {}).get("evidence", {}).get("store_page_images", False)
    
    redaction_stats = {"count": 0}
    
    # 1. Primary Insights evidence
    insights_path = job_dir / "primary" / "risk_arguments.jsonl"
    if insights_path.exists():
        insights = read_jsonl(insights_path)
        notes = []
        for i, ins in enumerate(insights):
            quote = ins.get("quote", "")
            if redact_enabled:
                quote = redact_text(quote, pii_patterns, redaction_stats)
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
        
        quote_links_path = job_dir / "primary" / "quote_links.jsonl"
        if quote_links_path.exists():
            import shutil
            dest_links = primary_dir / "quote_links.jsonl"
            shutil.copy(quote_links_path, dest_links)
            manifest.append({
                "path": "primary/quote_links.jsonl",
                "bytes": dest_links.stat().st_size,
                "sha256": sha256_of_file(dest_links),
                "source_artifact": "quote_links.jsonl",
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
                    snippet = redact_text(snippet, pii_patterns, redaction_stats)
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
                    snippet = redact_text(snippet, pii_patterns, redaction_stats)
                doc_evidences.append({
                    "field": f.get("field"),
                    "page": f.get("page"),
                    "file": f.get("file"),
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
                "contract": "facts",
                "anchors": [{"file": d.get("file"), "page": d.get("page")} for d in doc_evidences]
            })
            
        if store_page_images:
            try:
                import pdfplumber
                pages_dir = docs_dir / "pages"
                pages_dir.mkdir(exist_ok=True)
                
                pdf_paths = list((job_dir / "inputs" / "pdfs").glob("*.pdf"))
                if not pdf_paths:
                    project_root = Path(__file__).resolve().parent.parent.parent
                    pdf_paths = list((project_root / cfg.get("mock_paths", {}).get("pdf_dir", "mock_dbx/dbfs")).glob("*.pdf"))
                
                for pdf_file in pdf_paths:
                    try:
                        with pdfplumber.open(pdf_file) as pdf:
                            for i, page in enumerate(pdf.pages):
                                if i >= 3: break
                                im = page.to_image(resolution=72)
                                img_path = pages_dir / f"{pdf_file.stem}_page_{i+1}.jpg"
                                im.original.save(img_path, format="JPEG")
                                manifest.append({
                                    "path": f"docs/pages/{img_path.name}",
                                    "bytes": img_path.stat().st_size,
                                    "sha256": sha256_of_file(img_path),
                                    "source_artifact": pdf_file.name,
                                    "contract": "image"
                                })
                    except Exception:
                        pass
            except ImportError:
                pass

    # 4. Spike/Reversal summary
    signals_path = job_dir / "ingestor" / "signals.json"
    if signals_path.exists():
        try:
            with open(signals_path, "r") as f:
                sig = json.load(f)
            
            summary = {
                "circular_trading_risk": sig.get("circular_trading_risk", {}),
                "top_spikes": [],
                "top_reversals": sig.get("reversals", [])[:5]
            }
            # Add top spikes across all series
            all_spikes = []
            for series, s_list in sig.get("spikes", {}).items():
                for s in s_list:
                    all_spikes.append({**s, "series": series})
            summary["top_spikes"] = sorted(all_spikes, key=lambda x: x.get("z", 0), reverse=True)[:5]
            
            summary_file = docs_dir / "spike_reversal_summary.json"
            with open(summary_file, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2)
                
            manifest.append({
                "path": "docs/spike_reversal_summary.json",
                "bytes": summary_file.stat().st_size,
                "sha256": sha256_of_file(summary_file),
                "source_artifact": "signals.json",
                "contract": "facts"
            })
        except Exception:
            pass

    manifest_file = pack_dir / "evidence_manifest.json"
    with open(manifest_file, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
        
    if redact_enabled and redaction_stats["count"] > 0:
        try:
            from governance.provenance.provenance import append_metrics
            append_metrics(job_dir, "redaction", {"count": redaction_stats["count"]})
        except Exception:
            pass

    # Root manifest
    root_manifest = {}
    core_files = [
        "ingestor/facts.jsonl",
        "research/research_findings.jsonl",
        "primary/risk_arguments.jsonl",
        "decision_engine/decision_output.json",
        "metrics.json",
        "validation_aggregate.json",
        "provenance.json"
    ]
    for rel_path in core_files:
        fpath = job_dir / rel_path
        if fpath.exists():
            root_manifest[rel_path] = {
                "bytes": fpath.stat().st_size,
                "sha256": sha256_of_file(fpath)
            }
            
    with open(job_dir / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(root_manifest, f, indent=2)
