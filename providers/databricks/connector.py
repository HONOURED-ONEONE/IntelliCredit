import os
import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

class DatabricksConnector:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        
        if not self.host or not self.token or not self.http_path:
            raise ValueError("Missing required Databricks environment variables: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH")
            
        try:
            from databricks.sdk import WorkspaceClient
            self.client = WorkspaceClient(host=self.host, token=self.token)
        except ImportError:
            raise ImportError("databricks-sdk is required for live Databricks integration")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def list_pdfs(self, path: str) -> list[dict]:
        try:
            files = self.client.dbfs.list(path)
            # return basic info, mock reading files locally for simplicity unless we implement dbfs read
            return [{"path": f.path, "name": f.path.split('/')[-1]} for f in files if f.path.endswith('.pdf')]
        except Exception as e:
            if self.cfg.get("security", {}).get("redact_keys_in_logs", True):
                logger.error("Error listing PDFs in Databricks DBFS")
            else:
                logger.error(f"Error listing PDFs in Databricks DBFS: {e}")
            raise e

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def read_uc_table(self, catalog: str, schema: str, table: str) -> pd.DataFrame:
        try:
            from databricks import sql
            connection = sql.connect(
                server_hostname=self.host,
                http_path=self.http_path,
                access_token=self.token
            )
            query = f"SELECT * FROM {catalog}.{schema}.{table} LIMIT 10000"
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                return pd.DataFrame(rows, columns=cols)
        except Exception as e:
            if self.cfg.get("security", {}).get("redact_keys_in_logs", True):
                logger.error(f"Error reading UC table {catalog}.{schema}.{table}")
            else:
                logger.error(f"Error reading UC table {catalog}.{schema}.{table}: {e}")
            raise e
