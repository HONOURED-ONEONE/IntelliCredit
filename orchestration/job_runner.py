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
    Executes an end-to-end job.
    Transitions status: started -> ingestor -> research -> primary -> decision -> completed
    """
    project_root = Path(__file__).resolve().parent.parent
    config = load_config()
    output_root = config.get("paths", {}).get("output_root", "outputs/jobs")
    job_dir = project_root / output_root / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    
    status_file = job_dir / "status.json"
    logs_file = job_dir / "logs.txt"
    provenance_file = job_dir / "provenance.json"
    
    job_logger = logger.bind(job_id=job_id)
    handler_id = logger.add(logs_file, format="{time} | {level} | {message}", filter=lambda record: record["extra"].get("job_id") == job_id)
    
    try:
        current_status = {}
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
                "created_at": current_status.get("created_at", datetime.utcnow().isoformat() + "Z"),
                "updated_at": datetime.utcnow().isoformat() + "Z"
            }
            with open(status_file, "w") as f:
                json.dump(status, f, indent=2)
            job_logger.info(f"Status updated to: {stage}")

        if not current_status:
            update_status("started")
            from governance.provenance.provenance import update_provenance_timing
            provenance = {
                "job_id": job_id,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "payload_echo": payload,
                "config_snapshot": config,
                "timing": {},
                "models_used": config.get("llm", {}).get("model_map", {})
            }
            with open(provenance_file, "w") as f:
                json.dump(provenance, f, indent=2)
            job_logger.info("Provenance written.")
            
        stages = ["ingestor", "research", "primary", "decision"]
        
        from intelligence.ingestor import ingestor
        from intelligence.research import research_agent
        from intelligence.primary import primary_agent
        from intelligence.decision_engine import decision_engine
        from governance.provenance.provenance import update_provenance_timing
        
        module_map = {
            "ingestor": ingestor,
            "research": research_agent,
            "primary": primary_agent,
            "decision": decision_engine
        }

        for stage in stages:
            stage_dir = job_dir / (stage if stage != "decision" else "decision_engine")
            if stage_dir.exists() and (stage_dir / "validation_report.json").exists():
                job_logger.info(f"Stage {stage} outputs exist. Resume-skip.")
            else:
                update_status(stage)
                job_logger.info(f"Running {stage}...")
                module_map[stage].run(job_dir, config, payload)
                update_provenance_timing(job_dir, stage, datetime.utcnow().isoformat() + "Z")
                await asyncio.sleep(0.5)

        update_status("completed")
        job_logger.info("Job completed successfully.")
        
    except Exception as e:
        job_logger.error(f"Job failed: {e}")
    finally:
        logger.remove(handler_id)
