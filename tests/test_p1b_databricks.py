import pytest
import pandas as pd
from providers.databricks.schema_map import to_canonical_gst, to_canonical_bank

def test_to_canonical_gst():
    # UC might have "month", "turnover", "cgst_tax"
    df = pd.DataFrame({
        "Month": ["2023-01", "2023-02"],
        "Turnover": ["100", "200"],
        "CGST_Tax": ["10", "20"]
    })
    
    canonical = to_canonical_gst(df)
    assert list(canonical.columns) == ["date", "sales", "tax_paid"]
    assert canonical["sales"].tolist() == [100.0, 200.0]
    assert canonical["tax_paid"].tolist() == [10.0, 20.0]

def test_to_canonical_bank():
    # UC might have "Txn_Date", "Narration", "Credit", "Debit"
    df = pd.DataFrame({
        "Txn_Date": ["2023-01-01", "2023-01-02"],
        "Narration": ["Deposit", "Withdrawal"],
        "Credit": ["1000", ""],
        "Debit": ["", "500"]
    })
    
    canonical = to_canonical_bank(df)
    assert list(canonical.columns) == ["date", "description", "amount"]
    assert canonical["amount"].tolist() == [1000.0, -500.0]
