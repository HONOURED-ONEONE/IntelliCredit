import os
from loguru import logger
from .connector import DatabricksConnector
from .mock_connector import MockDatabricksConnector

def get_connector(cfg: dict, force_live: bool = False):
    enable_live = cfg.get("features", {}).get("enable_live_databricks", False) or force_live
    mode = cfg.get("integrations", {}).get("databricks", {}).get("mode", "mock")
    
    # Only attempt DatabricksConnector if feature enabled, mode live, and vars exist
    has_creds = bool(os.getenv("DATABRICKS_HOST") and os.getenv("DATABRICKS_TOKEN") and os.getenv("DATABRICKS_HTTP_PATH"))
    
    if enable_live and mode == "live" and has_creds:
        try:
            return DatabricksConnector(cfg)
        except Exception as e:
            logger.warning(f"Failed to initialize live Databricks connector, falling back to mock: {e}")
            return MockDatabricksConnector(cfg)
            
    if enable_live and mode == "live" and not has_creds:
        logger.warning("Live Databricks requested but missing required environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH). Falling back to mock.")
            
    return MockDatabricksConnector(cfg)
