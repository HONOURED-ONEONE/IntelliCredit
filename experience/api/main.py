import uuid
import json
import os
import shutil
import hashlib
from pathlib import Path
from typing import List, Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Request, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
import sys

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from orchestration.job_runner import run_job_async, load_config
from experience.api.schemas import (
    JobPayload, JobResponse, JobStatusResponse, JobResultsResponse, 
    FileInfo, FileNode, UploadEntry, UploadManifest
)

app = FastAPI(title="IntelliCredit API MVP")

config = load_config()
allowed_origins = config.get("security", {}).get("allowed_origins", ["*"])
max_req_mb = config.get("security", {}).get("max_request_mb", 10)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class LimitUploadSize(BaseHTTPMiddleware):
    def __init__(self, app, max_upload_size: int):
        super().__init__(app)
        self.max_upload_size = max_upload_size

    async def dispatch(self, request: Request, call_next):
        if request.method == "POST":
            if "content-length" in request.headers:
                content_length = int(request.headers["content-length"])
                if content_length > self.max_upload_size:
                    return JSONResponse(status_code=413, content={"detail": "Payload too large"})
        return await call_next(request)

app.add_middleware(LimitUploadSize, max_upload_size=max_req_mb * 1024 * 1024)

@app.get("/health/live")
async def health_live():
    return {"status": "ok", "version": "0.1.0-mvp"}

@app.get("/health/ready")
async def health_ready():
    config = load_config()
    out_dir = project_root / config.get("paths", {}).get("output_root", "outputs/jobs")
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        test_file = out_dir / ".ready_test"
        test_file.touch()
        test_file.unlink()
        write_ok = True
    except Exception:
        write_ok = False
        
    readiness = {
        "status": "ready" if write_ok else "not_ready",
        "write_access": write_ok,
        "llm_live": {
            "skipped": not config.get("features", {}).get("enable_live_llm", False),
            "ok": bool(os.getenv("OPENAI_API_KEY") or os.getenv("ANTHROPIC_API_KEY")),
            "reason": "Missing OPENAI_API_KEY or ANTHROPIC_API_KEY" if not bool(os.getenv("OPENAI_API_KEY") or os.getenv("ANTHROPIC_API_KEY")) else "OK"
        },
        "search_live": {
            "skipped": not config.get("features", {}).get("enable_live_search", False),
            "ok": bool(os.getenv("PPLX_API_KEY") or os.getenv("TAVILY_API_KEY") or os.getenv("BING_SUBSCRIPTION_KEY") or os.getenv("SERPAPI_API_KEY")),
            "reason": "Missing Search keys" if not bool(os.getenv("PPLX_API_KEY") or os.getenv("TAVILY_API_KEY") or os.getenv("BING_SUBSCRIPTION_KEY") or os.getenv("SERPAPI_API_KEY")) else "OK"
        },
        "databricks_live": {
            "skipped": not config.get("features", {}).get("enable_live_databricks", False),
            "ok": bool(os.getenv("DATABRICKS_HOST") and os.getenv("DATABRICKS_TOKEN")),
            "reason": "Missing DATABRICKS_HOST or DATABRICKS_TOKEN" if not bool(os.getenv("DATABRICKS_HOST") and os.getenv("DATABRICKS_TOKEN")) else "OK"
        }
    }
    
    if not write_ok:
        raise HTTPException(status_code=503, detail=readiness)
    return readiness

@app.post("/jobs", response_model=JobResponse)
async def create_job(payload: JobPayload, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    background_tasks.add_task(run_job_async, job_id, payload.model_dump())
    return JobResponse(job_id=job_id)

@app.post("/jobs/{job_id}/uploads", response_model=UploadManifest)
async def upload_job_inputs(
    job_id: str,
    gst_returns: Optional[UploadFile] = File(None),
    bank_transactions: Optional[UploadFile] = File(None),
    pdfs: List[UploadFile] = File(default=[])
):
    config = load_config()
    output_root = config.get("paths", {}).get("output_root", "outputs/jobs")
    job_dir = project_root / output_root / job_id
    
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found")
        
    inputs_dir = job_dir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)
    
    saved_entries = []
    
    def save_and_hash(upload_file, dest_path, field):
        if not upload_file.filename.lower().endswith(('.csv', '.pdf')):
            raise HTTPException(status_code=415, detail=f"Unsupported file type for {upload_file.filename}")
            
        sha256 = hashlib.sha256()
        with open(dest_path, "wb") as buffer:
            # Read in chunks to compute hash and save
            while True:
                chunk = upload_file.file.read(1024 * 1024) # 1MB chunks
                if not chunk: break
                sha256.update(chunk)
                buffer.write(chunk)
        
        saved_entries.append(UploadEntry(
            field=field,
            name=dest_path.name,
            bytes=dest_path.stat().st_size,
            sha256=sha256.hexdigest()
        ))

    if gst_returns:
        save_and_hash(gst_returns, inputs_dir / "gst_returns.csv", "gst_returns")
    if bank_transactions:
        save_and_hash(bank_transactions, inputs_dir / "bank_transactions.csv", "bank_transactions")
    if pdfs:
        pdf_dir = inputs_dir / "pdfs"
        pdf_dir.mkdir(exist_ok=True)
        for pdf in pdfs:
            # Sanitize name
            safe_name = "".join([c if c.isalnum() or c in "._-" else "_" for c in pdf.filename])
            save_and_hash(pdf, pdf_dir / safe_name, "pdfs")

    # Update provenance if it exists
    prov_file = job_dir / "provenance.json"
    if prov_file.exists():
        with open(prov_file, "r") as f:
            prov = json.load(f)
        prov["inputs_manifest"] = [e.model_dump() for e in saved_entries]
        with open(prov_file, "w") as f:
            json.dump(prov, f, indent=2)
            
    return UploadManifest(job_id=job_id, saved=saved_entries)

@app.get("/jobs/{job_id}/inputs", response_model=List[UploadEntry])
async def list_job_inputs(job_id: str):
    config = load_config()
    output_root = config.get("paths", {}).get("output_root", "outputs/jobs")
    inputs_dir = project_root / output_root / job_id / "inputs"
    
    if not inputs_dir.exists():
        return []
        
    entries = []
    for f in inputs_dir.rglob("*"):
        if f.is_file():
            field = "pdfs" if "pdfs" in str(f) else ("gst_returns" if "gst_returns" in f.name else "bank_transactions")
            
            # Compute hash
            sha256 = hashlib.sha256()
            with open(f, "rb") as bf:
                while True:
                    chunk = bf.read(1024 * 1024)
                    if not chunk: break
                    sha256.update(chunk)
                    
            entries.append(UploadEntry(
                field=field,
                name=f.name,
                bytes=f.stat().st_size,
                sha256=sha256.hexdigest()
            ))
    return entries

@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    config = load_config()
    output_root = config.get("paths", {}).get("output_root", "outputs/jobs")
    status_file = project_root / output_root / job_id / "status.json"
    
    if not status_file.exists():
        raise HTTPException(status_code=404, detail="Job not found")
        
    with open(status_file, "r") as f:
        status_data = json.load(f)
        
    return JobStatusResponse(**status_data)

@app.get("/jobs/{job_id}/metrics")
async def get_job_metrics(job_id: str):
    config = load_config()
    output_root = config.get("paths", {}).get("output_root", "outputs/jobs")
    metrics_file = project_root / output_root / job_id / "metrics.json"

    if not metrics_file.exists():
        return {}

    with open(metrics_file, "r") as f:
        return json.load(f)

@app.get("/metrics")
async def get_prometheus_metrics():
    config = load_config()
    if not config.get("metrics", {}).get("prometheus", {}).get("enabled", False):
        raise HTTPException(status_code=404, detail="Prometheus metrics not enabled")
    try:
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
        from fastapi.responses import Response
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
    except ImportError:
        raise HTTPException(status_code=503, detail="Prometheus not available")
@app.get("/jobs/{job_id}/provenance")
async def get_job_provenance(job_id: str):
    config = load_config()
    output_root = config.get("paths", {}).get("output_root", "outputs/jobs")
    prov_file = project_root / output_root / job_id / "provenance.json"
    
    if not prov_file.exists():
        return {}
        
    with open(prov_file, "r") as f:
        return json.load(f)

@app.get("/jobs/{job_id}/validation")
async def get_job_validation(job_id: str, stage: str = Query(..., regex="^(ingestor|research|primary|decision)$")):
    config = load_config()
    output_root = config.get("paths", {}).get("output_root", "outputs/jobs")
    val_file = project_root / output_root / job_id / f"{stage}_validation_report.json"
    
    if not val_file.exists():
        raise HTTPException(status_code=404, detail="Validation report not found")
        
    with open(val_file, "r") as f:
        return json.load(f)

@app.get("/jobs/{job_id}/validation/aggregate")
def get_validation_aggregate(job_id: str):
    config = load_config()
    output_root = config.get("paths", {}).get("output_root", "outputs/jobs")
    agg_file = project_root / output_root / job_id / "validation_aggregate.json"
    if not agg_file.exists():
        raise HTTPException(status_code=404, detail="Aggregate validation not found")
    try:
        with open(agg_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs/{job_id}/evidence")
async def get_job_evidence(job_id: str):
    config = load_config()
    output_root = config.get("paths", {}).get("output_root", "outputs/jobs")
    manifest_file = project_root / output_root / job_id / "evidence_pack" / "evidence_manifest.json"
    
    if not manifest_file.exists():
        raise HTTPException(status_code=404, detail="Evidence manifest not found")
        
    with open(manifest_file, "r") as f:
        return json.load(f)

@app.get("/results/{job_id}", response_model=JobResultsResponse)
async def get_job_results(job_id: str, subdir: str = Query(None), tree: bool = Query(False)):
    config = load_config()
    output_root = config.get("paths", {}).get("output_root", "outputs/jobs")
    base_dir = project_root / output_root / job_id
    
    if not base_dir.exists():
        raise HTTPException(status_code=404, detail="Job directory not found")
        
    target_dir = base_dir / subdir if subdir else base_dir
    
    if not target_dir.exists():
        return JobResultsResponse(job_id=job_id, files=[])
        
    if tree:
        def build_tree(path: Path) -> FileNode:
            node = FileNode(name=path.name, is_dir=path.is_dir())
            if path.is_dir():
                node.children = [build_tree(p) for p in sorted(path.iterdir())]
            else:
                node.size_bytes = path.stat().st_size
            return node
            
        tree_data = [build_tree(p) for p in sorted(target_dir.iterdir())]
        return JobResultsResponse(job_id=job_id, tree=tree_data)
    else:
        files = []
        for file_path in target_dir.rglob("*"):
            if file_path.is_file():
                rel_path = file_path.relative_to(base_dir)
                files.append(FileInfo(name=str(rel_path), size_bytes=file_path.stat().st_size))
                
        return JobResultsResponse(job_id=job_id, files=files)
