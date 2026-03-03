import json
import os
from pathlib import Path
from loguru import logger
import pandas as pd
from .normalizers import read_gst_csv, read_bank_csv
from .pdf_utils import extract_text_pages
from governance.validation.validators import write_validation_report
from governance.provenance.provenance import append_metrics

def run(job_dir: Path, cfg: dict, payload: dict) -> None:
    mode = payload.get("provider_mode") or cfg.get("providers", {}).get("mode", "mock")
    enable_live_llm = payload.get("enable_live_llm", cfg.get("features", {}).get("enable_live_llm", False))
    out_dir = job_dir / "ingestor"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    project_root = job_dir.parent.parent.parent
    
    if mode == "mock":
        gst_path = project_root / cfg.get("mock_paths", {}).get("gst_uc_csv", "mock_dbx/uc/gst_returns_sample.csv")
        bank_path = project_root / cfg.get("mock_paths", {}).get("bank_uc_csv", "mock_dbx/uc/bank_transactions_sample.csv")
        pdf_dir = project_root / cfg.get("mock_paths", {}).get("pdf_dir", "mock_dbx/dbfs")
    else:
        inputs_dir = job_dir / "inputs"
        gst_path = inputs_dir / "gst_returns.csv"
        bank_path = inputs_dir / "bank_transactions.csv"
        pdf_dir = inputs_dir / "pdfs"

    total_gst_sales = 0.0
    total_bank_inflow = 0.0
    total_bank_outflow = 0.0

    if gst_path.exists():
        df_gst = read_gst_csv(gst_path)
        if 'sales' in df_gst.columns:
            total_gst_sales = df_gst['sales'].sum()
        df_gst.to_csv(out_dir / "gst_returns.csv", index=False)
    else:
        pd.DataFrame().to_csv(out_dir / "gst_returns.csv", index=False)

    if bank_path.exists():
        df_bank = read_bank_csv(bank_path)
        if 'amount' in df_bank.columns:
            total_bank_inflow = df_bank[df_bank['amount'] > 0]['amount'].sum()
            total_bank_outflow = df_bank[df_bank['amount'] < 0]['amount'].sum()
        df_bank.to_csv(out_dir / "bank_transactions.csv", index=False)
    else:
        pd.DataFrame().to_csv(out_dir / "bank_transactions.csv", index=False)

    documents = []
    facts = [
        {"field": "total_gst_sales", "value": total_gst_sales},
        {"field": "total_bank_inflow", "value": total_bank_inflow},
        {"field": "total_bank_outflow", "value": total_bank_outflow}
    ]

    if pdf_dir.exists() and pdf_dir.is_dir():
        for pdf_file in pdf_dir.glob("*.pdf"):
            pages_text = extract_text_pages(pdf_file)
            
            # Live Vision extraction
            if enable_live_llm and os.getenv("OPENAI_API_KEY"):
                try:
                    import pdfplumber
                    from providers.llm.openai_client import OpenAIClient
                    client = OpenAIClient(api_key=os.getenv("OPENAI_API_KEY"))
                    vision_model = payload.get("vision_model") or cfg.get("llm", {}).get("model_map", {}).get("vision", "gpt-4o")
                    
                    schema = {
                        "type": "object",
                        "properties": {
                            "facts": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "field": {"type": "string"},
                                        "value": {"type": ["string", "number"]},
                                        "unit": {"type": "string"},
                                        "period": {"type": "string"},
                                        "evidence_snippet": {"type": "string"},
                                        "confidence": {"type": "number"}
                                    },
                                    "required": ["field", "value"]
                                }
                            }
                        },
                        "required": ["facts"]
                    }
                    
                    with pdfplumber.open(pdf_file) as pdf:
                        page_images = []
                        for i, page in enumerate(pdf.pages):
                            if i > 2: break # Limit to first 3 pages to save cost
                            im = page.to_image(resolution=72)
                            import io
                            buf = io.BytesIO()
                            im.original.save(buf, format='JPEG')
                            page_images.append(buf.getvalue())
                            
                        if page_images:
                            ext_list, metrics = client.vision_extract(
                                pages=page_images,
                                instructions="Extract key financial facts from these pages.",
                                schema=schema,
                                model=vision_model
                            )
                            append_metrics(job_dir, "ingestor_vision", metrics)
                            for fact in ext_list:
                                if isinstance(fact, dict) and "facts" in fact:
                                    for f in fact["facts"]:
                                        f["page"] = 1 # simplified
                                        facts.append(f)
                                elif isinstance(fact, dict) and "field" in fact:
                                    fact["page"] = 1
                                    facts.append(fact)
                except Exception as e:
                    logger.warning(f"Vision extraction failed: {e}")
                    
            documents.append({"file": pdf_file.name, "pages": pages_text})
            
    with open(out_dir / "documents.jsonl", "w") as f:
        for doc in documents:
            f.write(json.dumps(doc) + "\n")

    with open(out_dir / "facts.jsonl", "w") as f:
        for fact in facts:
            f.write(json.dumps(fact) + "\n")

    mismatch_val = abs(total_gst_sales - total_bank_inflow)
    threshold = 0.2 * total_bank_inflow if total_bank_inflow else 0
    mismatch = mismatch_val > threshold
    
    signals = {
        "mismatch": bool(mismatch),
        "mismatch_value": mismatch_val,
        "spike_reversal": False
    }
    with open(out_dir / "signals.json", "w") as f:
        json.dump(signals, f, indent=2)

    write_validation_report("ingestor", out_dir, {"docs_processed": len(documents), "facts_count": len(facts)})
