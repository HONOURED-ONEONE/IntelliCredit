import asyncio
import json
from datetime import datetime
from pathlib import Path
from loguru import logger
import yaml

def load_config() -> dict:
    """Loads the base configuration from config/base.yaml."""
    project_root = Path(__file__).resolve().parent.parent
    config_path = project_root / "config" / "base.yaml"
    if not config_path.exists():
        return {"paths": {"output_root": "outputs/jobs"}}
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

async def run_job_async(job_id: str, payload: dict):
    """
    Executes a minimal end-to-end job.
    Transitions status: started -> ingestor -> completed
    """
    project_root = Path(__file__).resolve().parent.parent
    config = load_config()
    output_root = config.get("paths", {}).get("output_root", "outputs/jobs")
    job_dir = project_root / output_root / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    
    status_file = job_dir / "status.json"
    logs_file = job_dir / "logs.txt"
    provenance_file = job_dir / "provenance.json"
    
    # Configure logger for this job
    job_logger = logger.bind(job_id=job_id)
    handler_id = logger.add(logs_file, format="{time} | {level} | {message}", filter=lambda record: record["extra"].get("job_id") == job_id)
    
    try:
        if status_file.exists():
            with open(status_file, "r") as f:
                current_status = json.load(f)
            if current_status.get("stage") == "completed":
                job_logger.info("Job already completed. Skipping.")
                return

        def update_status(stage: str):
            status = {
                "job_id": job_id,
                "stage": stage,
                "created_at": datetime.utcnow().isoformat() + "Z",
                "updated_at": datetime.utcnow().isoformat() + "Z"
            }
            if status_file.exists():
                with open(status_file, "r") as f:
                    old_status = json.load(f)
                status["created_at"] = old_status.get("created_at", status["created_at"])
            with open(status_file, "w") as f:
                json.dump(status, f, indent=2)
            job_logger.info(f"Status updated to: {stage}")

        # Start job
        update_status("started")
        await asyncio.sleep(1) # Simulate work
        
        # Write provenance
        provenance = {
            "job_id": job_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "payload_echo": payload,
            "config_snapshot": config
        }
        with open(provenance_file, "w") as f:
            json.dump(provenance, f, indent=2)
        job_logger.info("Provenance written.")
        
        # Ingestor stage
        update_status("ingestor")
        await asyncio.sleep(2) # Simulate work
        
        # Complete
        update_status("completed")
        job_logger.info("Job completed successfully.")
        
    except Exception as e:
        job_logger.error(f"Job failed: {e}")
    finally:
        logger.remove(handler_id)
