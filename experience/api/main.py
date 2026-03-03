import uuid
import json
from pathlib import Path
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
import sys

# Ensure project root is in path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from orchestration.job_runner import run_job_async, load_config
from experience.api.schemas import JobPayload, JobResponse, JobStatusResponse, JobResultsResponse, FileInfo

app = FastAPI(title="IntelliCredit API Phase 2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/jobs", response_model=JobResponse)
async def create_job(payload: JobPayload, background_tasks: BackgroundTasks):
    """Create a new job and run it in the background."""
    job_id = str(uuid.uuid4())
    background_tasks.add_task(run_job_async, job_id, payload.model_dump())
    return JobResponse(job_id=job_id)

@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Get the current status of a job."""
    config = load_config()
    output_root = config.get("paths", {}).get("output_root", "outputs/jobs")
    status_file = project_root / output_root / job_id / "status.json"
    
    if not status_file.exists():
        raise HTTPException(status_code=404, detail="Job not found")
        
    with open(status_file, "r") as f:
        status_data = json.load(f)
        
    return JobStatusResponse(**status_data)

@app.get("/results/{job_id}", response_model=JobResultsResponse)
async def get_job_results(job_id: str, subdir: str = Query(None)):
    """List all artifacts for a given job. Optional subdir to filter."""
    config = load_config()
    output_root = config.get("paths", {}).get("output_root", "outputs/jobs")
    base_dir = project_root / output_root / job_id
    
    if not base_dir.exists():
        raise HTTPException(status_code=404, detail="Job directory not found")
        
    target_dir = base_dir / subdir if subdir else base_dir
    
    if not target_dir.exists():
        return JobResultsResponse(job_id=job_id, files=[])
        
    files = []
    for file_path in target_dir.rglob("*"):
        if file_path.is_file():
            rel_path = file_path.relative_to(base_dir)
            files.append(FileInfo(name=str(rel_path), size_bytes=file_path.stat().st_size))
            
    return JobResultsResponse(job_id=job_id, files=files)
