from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List, Literal

class JobPayload(BaseModel):
    """Payload for starting a new job."""
    source: str = Field(default="system", description="Source text or identifier for the job")
    parameters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional parameters")
    company_name: Optional[str] = None
    promoter: Optional[str] = None
    notes: Optional[str] = None
    provider_mode: Optional[Literal["mock", "local_uploads"]] = None
    source_mode: Optional[Literal["mock", "local_uploads", "databricks_files", "databricks_tables"]] = None
    dbfs_path: Optional[str] = None
    gst_table: Optional[str] = None
    bank_table: Optional[str] = None
    use_mock_uc: Optional[bool] = None
    use_mock_pdfs: Optional[bool] = None
    enable_live_llm: Optional[bool] = False
    enable_live_search: Optional[bool] = False
    enable_live_databricks: Optional[bool] = False
    llm_provider: Optional[str] = None
    vision_model: Optional[str] = None
    reasoning_model: Optional[str] = None
    catalog: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")

class JobResponse(BaseModel):
    """Response when a job is created."""
    job_id: str

class JobStatusResponse(BaseModel):
    """Response for job status."""
    job_id: str
    stage: str
    outcome: Optional[str] = "PENDING"
    reasons: Optional[List[str]] = []
    created_at: str
    updated_at: str

class FileInfo(BaseModel):
    """Information about an output file."""
    name: str
    size_bytes: int

class FileNode(BaseModel):
    name: str
    is_dir: bool
    size_bytes: Optional[int] = None
    children: Optional[List['FileNode']] = None

class JobResultsResponse(BaseModel):
    """Response for job artifacts list."""
    job_id: str
    files: Optional[List[FileInfo]] = None
    tree: Optional[List[FileNode]] = None

class UploadEntry(BaseModel):
    field: Literal["gst_returns", "bank_transactions", "pdfs"]
    name: str
    bytes: int
    sha256: str

class UploadManifest(BaseModel):
    job_id: str
    saved: List[UploadEntry]
