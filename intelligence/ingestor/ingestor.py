import json
import shutil
from pathlib import Path
from loguru import logger
import pandas as pd
from .normalizers import read_gst_csv, read_bank_csv
from .pdf_utils import extract_text_pages
from governance.validation.validators import write_validation_report

def run(job_dir: Path, cfg: dict, payload: dict) -> None:
    mode = payload.get("provider_mode") or cfg.get("providers", {}).get("mode", "mock")
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
    if pdf_dir.exists() and pdf_dir.is_dir():
        for pdf_file in pdf_dir.glob("*.pdf"):
            pages = extract_text_pages(pdf_file)
            documents.append({"file": pdf_file.name, "pages": pages})
            
    with open(out_dir / "documents.jsonl", "w") as f:
        for doc in documents:
            f.write(json.dumps(doc) + "
")

    facts = [
        {"field": "total_gst_sales", "value": total_gst_sales},
        {"field": "total_bank_inflow", "value": total_bank_inflow},
        {"field": "total_bank_outflow", "value": total_bank_outflow}
    ]
    with open(out_dir / "facts.jsonl", "w") as f:
        for fact in facts:
            f.write(json.dumps(fact) + "
")

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

    write_validation_report("ingestor", out_dir, {"docs_processed": len(documents)})
