import pytest
import json
import pandas as pd
from pathlib import Path
from intelligence.ingestor import ingestor

def test_spike_reversal_run(tmp_path):
    job_dir = tmp_path / "job_spike_run"
    job_dir.mkdir()
    
    # Create synthetic data with some variance so IQR/MAD > 0
    dates = pd.date_range("2023-01-01", periods=12, freq="MS")
    gst_sales = [900, 1100, 950, 1050, 1000, 5000, 950, 1050, 1000, 900, 1100, 1000]
    # Reversal at 2023-07 (index 6)
    bank_amounts = [1000, 1000, 1000, 1000, 1000, 1000, -4500, 1000, 1000, 1000, 1000, 1000]
    
    inputs_dir = job_dir / "inputs"
    inputs_dir.mkdir()
    
    gst_df = pd.DataFrame({"date": dates, "sales": gst_sales})
    bank_df = pd.DataFrame({"date": dates, "amount": bank_amounts})
    
    gst_df.to_csv(inputs_dir / "gst_returns.csv", index=False)
    bank_df.to_csv(inputs_dir / "bank_transactions.csv", index=False)
    
    cfg = {
        "signals": {
            "spike": {"method": "mad", "z_threshold": 2.0, "rel_threshold": 0.5, "min_points": 6, "rolling_window": 6},
            "reversal": {"window_k": 2, "offset_ratio_min": 0.7},
            "weights": {"spike": 10, "reversal": 25, "cap": 100}
        },
        "features": {"enable_live_llm": False, "ocr": {"enabled": False}},
        "providers": {"mode": "local_uploads"},
        "paths": {"output_root": "outputs"}
    }
    
    payload = {"source_mode": "local_uploads"}
    
    ingestor.run(job_dir, cfg, payload)
    
    signals_file = job_dir / "ingestor" / "signals.json"
    assert signals_file.exists()
    
    with open(signals_file, "r") as f:
        sig = json.load(f)
        assert len(sig["spikes"]["gst_sales"]) > 0
        assert len(sig["reversals"]) > 0
        assert sig["circular_trading_risk"]["score"] > 0
