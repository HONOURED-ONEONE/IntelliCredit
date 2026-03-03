import pandas as pd
from pathlib import Path

def parse_amount(val) -> float:
    if pd.isna(val): return 0.0
    s = str(val).replace(',', '').replace('₹', '').strip()
    if s.startswith('(') and s.endswith(')'):
        s = '-' + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return 0.0

def read_gst_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.lower().strip() for c in df.columns]
    for col in ['sales', 'tax_paid']:
        if col in df.columns:
            df[col] = df[col].apply(parse_amount)
    return df

def read_bank_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.lower().strip() for c in df.columns]
    if 'amount' in df.columns:
        df['amount'] = df['amount'].apply(parse_amount)
    return df
