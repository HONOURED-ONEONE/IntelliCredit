from loguru import logger
from .connector import DatabricksConnector
from .mock_connector import MockDatabricksConnector

def get_connector(cfg: dict, force_live: bool = False):
    enable_live = cfg.get("features", {}).get("enable_live_databricks", False) or force_live
    mode = cfg.get("integrations", {}).get("databricks", {}).get("mode", "mock")
    
    if enable_live and mode == "live":
        try:
            return DatabricksConnector(cfg)
        except Exception as e:
            logger.warning(f"Failed to initialize live Databricks connector, falling back to mock: {e}")
            return MockDatabricksConnector(cfg)
            
    return MockDatabricksConnector(cfg)
