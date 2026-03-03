import json
import os
from pathlib import Path
from loguru import logger
import pandas as pd
from .normalizers import read_gst_csv, read_bank_csv
from .pdf_utils import extract_text_pages

def run(job_dir: Path, cfg: dict, payload: dict) -> None:
    source_mode = payload.get("source_mode") or payload.get("provider_mode") or cfg.get("providers", {}).get("mode", "mock")
    enable_live_llm = payload.get("enable_live_llm", cfg.get("features", {}).get("enable_live_llm", False))
    enable_live_databricks = payload.get("enable_live_databricks", cfg.get("features", {}).get("enable_live_databricks", False))
    
    out_dir = job_dir / "ingestor"
    out_dir.mkdir(parents=True, exist_ok=True)
    project_root = job_dir.parent.parent.parent

    # Get Databricks connector
    from providers.databricks.factory import get_connector
    dbx = get_connector(cfg, force_live=enable_live_databricks)

    gst_df = pd.DataFrame()
    bank_df = pd.DataFrame()
    pdf_paths = []

    if source_mode == "mock":
        gst_path = project_root / cfg.get("mock_paths", {}).get("gst_uc_csv", "mock_dbx/uc/gst_returns_sample.csv")
        bank_path = project_root / cfg.get("mock_paths", {}).get("bank_uc_csv", "mock_dbx/uc/bank_transactions_sample.csv")
        pdf_dir = project_root / cfg.get("mock_paths", {}).get("pdf_dir", "mock_dbx/dbfs")
        if gst_path.exists(): gst_df = read_gst_csv(gst_path)
        if bank_path.exists(): bank_df = read_bank_csv(bank_path)
        if pdf_dir.exists(): pdf_paths = list(pdf_dir.glob("*.pdf"))

    elif source_mode == "local_uploads":
        inputs_dir = job_dir / "inputs"
        gst_path = inputs_dir / "gst_returns.csv"
        bank_path = inputs_dir / "bank_transactions.csv"
        pdf_dir = inputs_dir / "pdfs"
        if gst_path.exists(): gst_df = read_gst_csv(gst_path)
        if bank_path.exists(): bank_df = read_bank_csv(bank_path)
        if pdf_dir.exists(): pdf_paths = list(pdf_dir.glob("*.pdf"))

    elif source_mode == "databricks_tables":
        catalog = payload.get("catalog") or cfg.get("integrations", {}).get("databricks", {}).get("catalog", "main")
        schema = payload.get("schema") or cfg.get("integrations", {}).get("databricks", {}).get("schema", "credit")
        gst_table = payload.get("gst_table") or cfg.get("integrations", {}).get("databricks", {}).get("gst_table", "gst_returns")
        bank_table = payload.get("bank_table") or cfg.get("integrations", {}).get("databricks", {}).get("bank_table", "bank_transactions")
        
        try:
            gst_df = dbx.read_uc_table(catalog, schema, gst_table)
        except Exception as e:
            logger.warning(f"Failed to read GST table {gst_table}: {e}")
            with open(out_dir / "stage_note.txt", "a") as f: f.write("databricks_live_error=true\n")
            
        try:
            bank_df = dbx.read_uc_table(catalog, schema, bank_table)
        except Exception as e:
            logger.warning(f"Failed to read Bank table {bank_table}: {e}")
            with open(out_dir / "stage_note.txt", "a") as f: f.write("databricks_live_error=true\n")
            
        # Optional: still load mock PDFs if configured so we have docs
        pdf_dir = project_root / cfg.get("mock_paths", {}).get("pdf_dir", "mock_dbx/dbfs")
        if pdf_dir.exists(): pdf_paths = list(pdf_dir.glob("*.pdf"))

    elif source_mode == "databricks_files":
        dbfs_path = payload.get("dbfs_path") or cfg.get("integrations", {}).get("databricks", {}).get("files_root", "dbfs:/Shared/credit_docs")
        try:
            pdf_files_info = dbx.list_pdfs(dbfs_path)
            # For this MVP, we assume local mock DBFS dir actually holds these files or we just use mock fallback
            # because downloading from DBFS is complex. We'll use mock files locally matching the names.
            pdf_dir = project_root / cfg.get("mock_paths", {}).get("pdf_dir", "mock_dbx/dbfs")
            for info in pdf_files_info:
                local_path = pdf_dir / info["name"]
                if local_path.exists():
                    pdf_paths.append(local_path)
        except Exception as e:
            logger.warning(f"Failed to list PDFs from DBFS {dbfs_path}: {e}")
            with open(out_dir / "stage_note.txt", "a") as f: f.write("databricks_live_error=true\n")

        # Optional: load mock tables for data
        gst_path = project_root / cfg.get("mock_paths", {}).get("gst_uc_csv", "mock_dbx/uc/gst_returns_sample.csv")
        bank_path = project_root / cfg.get("mock_paths", {}).get("bank_uc_csv", "mock_dbx/uc/bank_transactions_sample.csv")
        if gst_path.exists(): gst_df = read_gst_csv(gst_path)
        if bank_path.exists(): bank_df = read_bank_csv(bank_path)

    # Calculate totals
    total_gst_sales = float(gst_df['sales'].sum()) if not gst_df.empty and 'sales' in gst_df.columns else 0.0
    total_bank_inflow = float(bank_df[bank_df['amount'] > 0]['amount'].sum()) if not bank_df.empty and 'amount' in bank_df.columns else 0.0
    total_bank_outflow = float(bank_df[bank_df['amount'] < 0]['amount'].sum()) if not bank_df.empty and 'amount' in bank_df.columns else 0.0

    gst_df.to_csv(out_dir / "gst_returns.csv", index=False)
    bank_df.to_csv(out_dir / "bank_transactions.csv", index=False)

    documents = []
    facts = [
        {"field": "total_gst_sales", "value": total_gst_sales},
        {"field": "total_bank_inflow", "value": total_bank_inflow},
        {"field": "total_bank_outflow", "value": total_bank_outflow}
    ]

    for pdf_file in pdf_paths:
        pages_text = extract_text_pages(pdf_file)
        
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
                        if i > 2: break
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
                        for fact in ext_list:
                            if isinstance(fact, dict) and "facts" in fact:
                                for f in fact["facts"]:
                                    f["page"] = 1
                                    facts.append(f)
                            elif isinstance(fact, dict) and "field" in fact:
                                fact["page"] = 1
                                facts.append(fact)
            except Exception as e:
                logger.warning(f"Vision extraction failed: {e}")
                
        documents.append({"file": pdf_file.name, "pages": pages_text})
            
    with open(out_dir / "documents.jsonl", "w", encoding="utf-8") as f:
        for doc in documents:
            f.write(json.dumps(doc) + "\n")

    with open(out_dir / "facts.jsonl", "w", encoding="utf-8") as f:
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
    with open(out_dir / "signals.json", "w", encoding="utf-8") as f:
        json.dump(signals, f, indent=2)
