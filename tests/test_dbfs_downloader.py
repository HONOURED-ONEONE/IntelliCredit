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
    with patch("os.getenv", return_value="dummy"):
        with patch("databricks.sdk.WorkspaceClient"):
            provider = DatabricksConnector(cfg)
    
    mock_reader = MagicMock()
    mock_reader.read.return_value = b"live content"
    
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_reader
    
    provider.client.dbfs.download = MagicMock(return_value=mock_ctx)
    
    dest_path = tmp_path / "live_dest.pdf"
    provider.download_dbfs_file("dbfs:/live.pdf", dest_path)
    
    assert dest_path.read_bytes() == b"live content"
