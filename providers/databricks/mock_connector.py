import pandas as pd
from pathlib import Path
from loguru import logger

class MockDatabricksConnector:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.project_root = Path(__file__).resolve().parent.parent.parent

    def list_pdfs(self, path: str) -> list[dict]:
        # path is ignored, map to mock_dbx/dbfs
        pdf_dir = self.project_root / self.cfg.get("mock_paths", {}).get("pdf_dir", "mock_dbx/dbfs")
        if not pdf_dir.exists():
            logger.warning(f"Mock DBFS path not found: {pdf_dir}")
            return []
        
        return [{"path": str(f), "name": f.name} for f in pdf_dir.glob("*.pdf")]

    def download_dbfs_file(self, remote_path: str, dest_path: Path) -> Path:
        import shutil
        pdf_dir = self.project_root / self.cfg.get("mock_paths", {}).get("pdf_dir", "mock_dbx/dbfs")
        filename = remote_path.split("/")[-1]
        src_path = pdf_dir / filename
        if src_path.exists():
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(src_path, dest_path)
            return dest_path
        raise FileNotFoundError(f"Mock file not found: {src_path}")

    def read_uc_table(self, catalog: str, schema: str, table: str) -> pd.DataFrame:
        # map to mock_dbx/uc/*.csv
        if "gst" in table.lower():
            csv_path = self.project_root / self.cfg.get("mock_paths", {}).get("gst_uc_csv", "mock_dbx/uc/gst_returns_sample.csv")
        else:
            csv_path = self.project_root / self.cfg.get("mock_paths", {}).get("bank_uc_csv", "mock_dbx/uc/bank_transactions_sample.csv")
            
        if not csv_path.exists():
            logger.warning(f"Mock UC table CSV not found: {csv_path}")
            return pd.DataFrame()
            
        return pd.read_csv(csv_path)
