import pytest
from pathlib import Path
from providers.databricks.connector import DatabricksConnector
from providers.databricks.mock_connector import MockDatabricksConnector
from unittest.mock import patch, MagicMock

def test_dbfs_downloader_mock(tmp_path):
    cfg = {"mock_paths": {"pdf_dir": str(tmp_path)}}
    provider = MockDatabricksConnector(cfg)
    
    # Create mock file
    mock_pdf = tmp_path / "mock_file.pdf"
    mock_pdf.write_text("mock content")
    
    dest_path = tmp_path / "dest.pdf"
    result = provider.download_dbfs_file("dbfs:/mock_file.pdf", dest_path)
    
    assert result == dest_path
    assert dest_path.read_text() == "mock content"

def test_dbfs_downloader_live(tmp_path):
    cfg = {}
    def mock_getenv(key, default=None):
        if key in ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH"]:
            return "dummy"
        return default
    with patch("os.getenv", side_effect=mock_getenv):
        with patch("databricks.sdk.WorkspaceClient"):
            provider = DatabricksConnector(cfg)
    
    mock_reader = MagicMock()
    mock_reader.read.side_effect = [b"live content", b""]
    
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_reader
    
    provider.client.dbfs.download = MagicMock(return_value=mock_ctx)
    
    dest_path = tmp_path / "live_dest.pdf"
    provider.download_dbfs_file("dbfs:/live.pdf", dest_path)
    
    assert dest_path.read_bytes() == b"live content"
