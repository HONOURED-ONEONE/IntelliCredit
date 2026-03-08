import json
import os
from pathlib import Path
from loguru import logger
import pandas as pd
from .normalizers import read_gst_csv, read_bank_csv
from .pdf_utils import extract_text_pages
from governance.provenance.provenance import append_metrics

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
        
        from providers.databricks.schema_map import to_canonical_gst, to_canonical_bank
        
        try:
            raw_gst = dbx.read_uc_table(catalog, schema, gst_table)
            gst_df = to_canonical_gst(raw_gst)
        except Exception as e:
            logger.warning(f"Failed to read GST table {gst_table}: {e}")
            with open(out_dir / "stage_note.txt", "a") as f: f.write("databricks_live_error=true\n")
            
        try:
            raw_bank = dbx.read_uc_table(catalog, schema, bank_table)
            bank_df = to_canonical_bank(raw_bank)
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
            inputs_dir = job_dir / "inputs"
            pdf_dir = inputs_dir / "pdfs"
            pdf_dir.mkdir(parents=True, exist_ok=True)
            for info in pdf_files_info:
                remote_path = info["path"]
                local_path = pdf_dir / info["name"]
                try:
                    dbx.download_dbfs_file(remote_path, local_path)
                    pdf_paths.append(local_path)
                except Exception as e:
                    logger.warning(f"Failed to download {remote_path}: {e}")
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

    ocr_enabled = cfg.get("features", {}).get("ocr", {}).get("enabled", False)
    cleanup_enabled = cfg.get("features", {}).get("cleanup", {}).get("enabled", False)

    documents = []
    facts = [
        {"field": "total_gst_sales", "value": total_gst_sales, "page": 1, "file": "gst_returns.csv", "evidence_snippet": "Computed from GST DB/CSV"},
        {"field": "total_bank_inflow", "value": total_bank_inflow, "page": 1, "file": "bank_transactions.csv", "evidence_snippet": "Computed from Bank DB/CSV"},
        {"field": "total_bank_outflow", "value": total_bank_outflow, "page": 1, "file": "bank_transactions.csv", "evidence_snippet": "Computed from Bank DB/CSV"}
    ]

    for pdf_file in pdf_paths:
        pages_text = extract_text_pages(pdf_file)
        
        if ocr_enabled:
            from providers.ocr.tesseract import available as ocr_available, image_from_pdf_page, ocr_image
            from providers.ocr.cleanup import cleanup_image
            if ocr_available():
                for page_data in pages_text:
                    if len(page_data.get("text", "").strip()) < 50:
                        img = image_from_pdf_page(str(pdf_file), page_data["page"] - 1)
                        if img:
                            img = cleanup_image(img, enabled=cleanup_enabled)
                            ocr_txt = ocr_image(img)
                            if ocr_txt:
                                page_data["text"] = page_data.get("text", "") + "\n" + ocr_txt
        
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
                        append_metrics(job_dir, f"ingestor_vision_{pdf_file.stem}", metrics)
                        for fact in ext_list:
                            if isinstance(fact, dict) and "facts" in fact:
                                for f in fact["facts"]:
                                    f["page"] = 1
                                    f["file"] = pdf_file.name
                                    if "evidence_snippet" not in f:
                                        f["evidence_snippet"] = pages_text[0]["text"][:300] if pages_text else "Extracted via Vision API"
                                    facts.append(f)
                            elif isinstance(fact, dict) and "field" in fact:
                                fact["page"] = 1
                                fact["file"] = pdf_file.name
                                if "evidence_snippet" not in fact:
                                    fact["evidence_snippet"] = pages_text[0]["text"][:300] if pages_text else "Extracted via Vision API"
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
    
    # --- WORKSTREAM A: Spike & Reversal Heuristics ---
    s_cfg = cfg.get("signals", {})
    spike_cfg = s_cfg.get("spike", {})
    rev_cfg = s_cfg.get("reversal", {})
    weights = s_cfg.get("weights", {})
    
    spikes = {"gst_sales": [], "bank_inflow": [], "bank_outflow": [], "bank_net": []}
    reversals = []
    risk_score = 0
    drivers = []

    def to_monthly(df, val_col):
        if df.empty or "date" not in df.columns or val_col not in df.columns:
            return pd.Series(dtype=float)
        temp = df.copy()
        temp["date"] = pd.to_datetime(temp["date"], errors="coerce")
        temp = temp.dropna(subset=["date"])
        if temp.empty: return pd.Series(dtype=float)
        # Resample to month-end and format as YYYY-MM
        s = temp.set_index("date")[val_col].resample("ME").sum()
        s.index = s.index.strftime("%Y-%m")
        return s

    gst_monthly = to_monthly(gst_df, "sales")
    bank_in = bank_df[bank_df["amount"] > 0] if not bank_df.empty else pd.DataFrame()
    bank_out = bank_df[bank_df["amount"] < 0].copy() if not bank_df.empty else pd.DataFrame()
    if not bank_out.empty: bank_out["amount"] = bank_out["amount"].abs()
    
    bank_in_monthly = to_monthly(bank_in, "amount")
    bank_out_monthly = to_monthly(bank_out, "amount")
    
    # bank_net = inflow - outflow
    all_months = sorted(list(set(bank_in_monthly.index) | set(bank_out_monthly.index)))
    bank_net_monthly = pd.Series(0.0, index=all_months)
    for m in all_months:
        bank_net_monthly[m] = bank_in_monthly.get(m, 0.0) - bank_out_monthly.get(m, 0.0)

    series_map = {
        "gst_sales": gst_monthly,
        "bank_inflow": bank_in_monthly,
        "bank_outflow": bank_out_monthly,
        "bank_net": bank_net_monthly
    }

    def detect_spikes_internal(series, cfg_obj):
        if len(series) < cfg_obj.get("min_points", 6):
            return []
        method = cfg_obj.get("method", "mad")
        z_thresh = cfg_obj.get("z_threshold", 3.0)
        rel_thresh = cfg_obj.get("rel_threshold", 0.6)
        win = cfg_obj.get("rolling_window", 6)
        
        found = []
        global_median = series.median()
        global_mad = (series - global_median).abs().median()
        global_iqr = series.quantile(0.75) - series.quantile(0.25)
        
        for i in range(len(series)):
            val = float(series.iloc[i])
            # trailing median baseline
            baseline = series.iloc[max(0, i-win):i].median() if i > 0 else global_median
            rel_change = (val - baseline) / baseline if baseline > 0 else (val if val > 0 else 0)
            
            z = 0
            if method == "mad":
                if global_mad > 0:
                    z = 0.6745 * (val - global_median) / global_mad
                elif global_iqr > 0:
                    z = (val - global_median) / global_iqr
                else:
                    z = 0
            else:
                z = (val - global_median) / global_iqr if global_iqr > 0 else 0
                
            if rel_change >= rel_thresh and z >= z_thresh:
                found.append({
                    "period": str(series.index[i]),
                    "value": val,
                    "z": float(round(z, 2)),
                    "method": method,
                    "rel_change": float(round(rel_change, 2))
                })
        return found

    # 1. Spikes
    for name, s in series_map.items():
        spikes[name] = detect_spikes_internal(s, spike_cfg)
    
    # 2. Reversals
    window_k = rev_cfg.get("window_k", 2)
    offset_min = rev_cfg.get("offset_ratio_min", 0.7)
    
    for lead_name, lead_spikes in spikes.items():
        for ls in lead_spikes:
            t_str = ls["period"]
            if t_str not in series_map[lead_name].index: continue
            t_idx = list(series_map[lead_name].index).index(t_str)
            lead_val = ls["value"]
            
            for follow_name, f_series in series_map.items():
                if lead_name == follow_name: continue
                for lag in range(1, window_k + 1):
                    if t_idx + lag >= len(f_series): break
                    f_period = f_series.index[t_idx + lag]
                    f_val = float(f_series.iloc[t_idx + lag])
                    f_baseline = f_series.iloc[max(0, t_idx-5):t_idx+1].median()
                    f_move = f_val - f_baseline
                    
                    ratio = abs(f_move) / abs(lead_val) if lead_val != 0 else 0
                    ratio = min(ratio, 1.5)
                    
                    is_offset = False
                    if lead_name in ["gst_sales", "bank_inflow"]:
                        if (follow_name == "bank_outflow" and f_move > 0) or \
                           (follow_name == "bank_net" and f_move < 0):
                            is_offset = True
                    elif lead_name == "bank_outflow":
                        if (follow_name == "bank_inflow" and f_move < 0) or \
                           (follow_name == "gst_sales" and f_move < 0):
                            is_offset = True
                    elif lead_name == "bank_net":
                        if (lead_val > 0 and f_move < 0) or (lead_val < 0 and f_move > 0):
                            is_offset = True
                            
                    if is_offset and ratio >= offset_min:
                        reversals.append({
                            "lead_series": lead_name, "lead_period": t_str,
                            "follow_series": follow_name, "follow_period": f_period,
                            "lead_value": lead_val, "follow_value": f_val,
                            "offset_ratio": round(ratio, 2), "lag": lag,
                            "note": f"{lead_name} spike followed by {follow_name} {'offset'}"
                        })
                        break

    # 3. Risk Score
    unique_spike_months = set()
    for slist in spikes.values():
        for sp in slist: unique_spike_months.add(sp["period"])
    
    s_count = len(unique_spike_months)
    r_count = len(reversals)
    risk_score = min(weights.get("cap", 100), weights.get("spike", 10)*s_count + weights.get("reversal", 25)*r_count)
    
    if s_count > 0: drivers.append(f"Detected {s_count} unique months with spikes across series")
    if r_count > 0: drivers.append(f"Detected {r_count} reversal events (offset follow-ups)")
    if risk_score >= 50: drivers.append("High circular trading risk pattern detected")

    signals = {
        "mismatch": bool(mismatch),
        "mismatch_value": mismatch_val,
        "spikes": spikes,
        "reversals": reversals,
        "circular_trading_risk": {
            "score": int(risk_score),
            "drivers": drivers
        }
    }
    with open(out_dir / "signals.json", "w", encoding="utf-8") as f:
        json.dump(signals, f, indent=2)
