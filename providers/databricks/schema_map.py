import pandas as pd
from loguru import logger

def to_canonical_gst(df: pd.DataFrame) -> pd.DataFrame:
    """Map arbitrary UC GST table schema to canonical format (date, sales, tax_paid)."""
    if df.empty:
        return pd.DataFrame(columns=["date", "sales", "tax_paid"])
        
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()
    
    # Map columns heuristically
    col_map = {}
    for c in df.columns:
        if 'date' in c or 'month' in c or 'period' in c:
            if 'date' not in col_map: col_map['date'] = c
        elif 'sale' in c or 'revenue' in c or 'turnover' in c:
            if 'sales' not in col_map: col_map['sales'] = c
        elif 'tax' in c or 'gst' in c or 'cgst' in c or 'sgst' in c or 'igst' in c:
            if 'tax_paid' not in col_map: col_map['tax_paid'] = c
            
    mapped_df = pd.DataFrame()
    mapped_df['date'] = df[col_map['date']] if 'date' in col_map else pd.Series(dtype='object')
    mapped_df['sales'] = df[col_map['sales']] if 'sales' in col_map else pd.Series(dtype='float64')
    mapped_df['tax_paid'] = df[col_map['tax_paid']] if 'tax_paid' in col_map else pd.Series(dtype='float64')
    
    # Missing columns warning
    missing = [c for c in ["date", "sales", "tax_paid"] if c not in col_map]
    if missing:
        logger.warning(f"Unmapped columns in GST data, synthesized zeros for: {missing}")
        for c in missing:
            if c == 'date':
                mapped_df[c] = ""
            else:
                mapped_df[c] = 0.0
                
    # Normalize numeric columns
    for c in ['sales', 'tax_paid']:
        mapped_df[c] = pd.to_numeric(mapped_df[c], errors='coerce').fillna(0.0)
        
    return mapped_df

def to_canonical_bank(df: pd.DataFrame) -> pd.DataFrame:
    """Map arbitrary UC Bank table schema to canonical format (date, description, amount)."""
    if df.empty:
        return pd.DataFrame(columns=["date", "description", "amount"])
        
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()
    
    col_map = {}
    for c in df.columns:
        if 'date' in c or 'time' in c:
            if 'date' not in col_map: col_map['date'] = c
        elif 'desc' in c or 'narration' in c or 'particulars' in c:
            if 'description' not in col_map: col_map['description'] = c
        elif 'amount' in c or 'value' in c or 'balance' not in c:
            if 'amount' not in col_map: col_map['amount'] = c
            
    # Handle separate credit/debit columns
    credit_col = next((c for c in df.columns if 'credit' in c or 'deposit' in c), None)
    debit_col = next((c for c in df.columns if 'debit' in c or 'withdrawal' in c), None)
    
    mapped_df = pd.DataFrame()
    mapped_df['date'] = df[col_map['date']] if 'date' in col_map else pd.Series(dtype='object')
    mapped_df['description'] = df[col_map['description']] if 'description' in col_map else pd.Series(dtype='object')
    
    if credit_col and debit_col and 'amount' not in col_map:
        # synthesize amount
        c_series = pd.to_numeric(df[credit_col], errors='coerce').fillna(0.0)
        d_series = pd.to_numeric(df[debit_col], errors='coerce').fillna(0.0)
        mapped_df['amount'] = c_series - d_series
        logger.info("Synthesized 'amount' from separate credit/debit columns.")
    elif 'amount' in col_map:
        mapped_df['amount'] = pd.to_numeric(df[col_map['amount']], errors='coerce').fillna(0.0)
    else:
        mapped_df['amount'] = pd.Series(dtype='float64')
        
    missing = [c for c in ["date", "description", "amount"] if c not in mapped_df.columns or mapped_df[c].isna().all()]
    if missing:
        logger.warning(f"Unmapped columns in Bank data, synthesized zeros for: {missing}")
        for c in missing:
            if c == 'date' or c == 'description':
                mapped_df[c] = ""
            else:
                mapped_df[c] = 0.0

    return mapped_df
