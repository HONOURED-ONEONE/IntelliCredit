from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List

class JobPayload(BaseModel):
    """Payload for starting a new job."""
    source: str = Field(..., description="Source text or identifier for the job")
    parameters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional parameters")

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
