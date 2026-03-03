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
    use_mock_uc: Optional[bool] = None
    use_mock_pdfs: Optional[bool] = None
    enable_live_llm: Optional[bool] = False
    enable_live_search: Optional[bool] = False
    llm_provider: Optional[str] = None
    vision_model: Optional[str] = None
    reasoning_model: Optional[str] = None

class JobResponse(BaseModel):
    """Response when a job is created."""
    job_id: str

class JobStatusResponse(BaseModel):
    """Response for job status."""
    job_id: str
    stage: str
    created_at: str
    updated_at: str

class FileInfo(BaseModel):
    """Information about an output file."""
    name: str
    size_bytes: int

class JobResultsResponse(BaseModel):
    """Response for job artifacts list."""
    job_id: str
    files: List[FileInfo]
