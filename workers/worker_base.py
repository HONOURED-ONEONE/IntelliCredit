import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from loguru import logger
import redis

import sys
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from orchestration.job_runner import load_config
from governance.audit.metrics import setup_job_logger
from governance.provenance.provenance import mark_stage
from governance.guardrails.policies import apply_gates

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def update_status(job_dir: Path, job_id: str, stage: str, outcome: str = "PENDING", reasons: list = None, job_logger=None):
    status_file = job_dir / "status.json"
    current_status = {}
    if status_file.exists():
        with open(status_file, "r") as f:
            try:
                current_status = json.load(f)
            except:
                pass

    status = {
        "job_id": job_id,
        "stage": stage,
        "outcome": outcome,
        "reasons": reasons or [],
        "created_at": current_status.get("created_at", _now_iso()),
        "updated_at": _now_iso()
    }
    
    with open(status_file, "w") as f:
        json.dump(status, f, indent=2)
        
    if job_logger:
        job_logger.info(f"Status updated to: {stage} (outcome: {outcome})")
        
    if stage == "completed":
        try:
            from governance.observability.prom import record_job_outcome
            record_job_outcome(outcome)
        except Exception:
            pass

class WorkerBase:
    def __init__(self, queue_name: str, next_queue_name: str, stage_name: str):
        self.queue_name = queue_name
        self.next_queue_name = next_queue_name
        self.stage_name = stage_name
        
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        self.redis_client = redis.from_url(redis_url, decode_responses=True)
        self.config = load_config()
        self.output_root = self.config.get("paths", {}).get("output_root", "outputs/jobs")

    def publish_event(self, queue: str, data: dict):
        if self.redis_client and queue:
            self.redis_client.lpush(queue, json.dumps(data))
            
    def process_message(self, message: str):
        try:
            data = json.loads(message)
            job_id = data.get("job_id")
            if not job_id:
                logger.error("No job_id in message")
                return
            
            job_dir = project_root / self.output_root / job_id
            job_dir.mkdir(parents=True, exist_ok=True)
            
            job_logger = setup_job_logger(job_dir, job_id, self.config)
            
            # Load payload
            payload_file = job_dir / "payload.json"
            payload = {}
            if payload_file.exists():
                with open(payload_file, "r") as f:
                    payload = json.load(f)
                    
            status_file = job_dir / "status.json"
            if status_file.exists():
                with open(status_file, "r") as f:
                    try:
                        status_data = json.load(f)
                        if status_data.get("stage") == "completed":
                            job_logger.info("Job already completed. Skipping.")
                            return
                    except:
                        pass
            else:
                if self.stage_name == "ingestor":
                    update_status(job_dir, job_id, "started", job_logger=job_logger)
                    from governance.provenance.provenance import start_run
                    start_run(job_dir, self.config, payload)
                    job_logger.info("Run started and provenance initialized.")

            # Idempotency check
            val_report_path = job_dir / f"{self.stage_name}_validation_report.json"
            can_skip = False
            expected_outputs = self.get_expected_outputs()
            if val_report_path.exists():
                with open(val_report_path, "r", encoding="utf-8") as f:
                    try:
                        rep = json.load(f)
                        if rep.get("schema_ok", False):
                            outputs_exist = all((job_dir / out).exists() for out in expected_outputs)
                            if outputs_exist:
                                can_skip = True
                    except:
                        pass
                        
            if can_skip:
                job_logger.info(f"Stage {self.stage_name} outputs and valid report exist. Resume-skip.")
                if self.next_queue_name:
                    self.publish_event(self.next_queue_name, {"job_id": job_id})
                return
                
            update_status(job_dir, job_id, self.stage_name, job_logger=job_logger)
            job_logger.info(f"Running {self.stage_name}...")
            
            start_time = _now_iso()
            
            try:
                self.run_stage(job_dir, payload, job_logger)
                
                # Metadata augmentation
                contracts_version = self.config.get("governance", {}).get("contracts_version", "0.1.0")
                for out_file in expected_outputs:
                    out_path = job_dir / out_file
                    if out_path.exists():
                        if out_path.suffix == ".jsonl":
                            from data_layer.contracts.utils import read_jsonl
                            items = read_jsonl(out_path)
                            for item in items:
                                item["schema_version"] = contracts_version
                                item["source"] = f"{self.stage_name}_agent"
                            with open(out_path, "w", encoding="utf-8") as f:
                                for item in items:
                                    f.write(json.dumps(item) + "\n")
                        elif out_path.suffix == ".json":
                            with open(out_path, "r", encoding="utf-8") as f:
                                try:
                                    data = json.load(f)
                                except:
                                    data = None
                            if isinstance(data, dict):
                                data["schema_version"] = contracts_version
                                data["source"] = f"{self.stage_name}_agent"
                                with open(out_path, "w", encoding="utf-8") as f:
                                    json.dump(data, f, indent=2)

            except Exception as e:
                job_logger.error(f"Error in {self.stage_name}: {e}")
                
            # Validation
            val_report = self.validate_stage(job_dir)
            job_logger.info(f"{self.stage_name} validation ok: {val_report['schema_ok']}")
            
            gate_res = apply_gates(job_dir, self.config)
            
            if gate_res["action"] != "ALLOW" or not val_report["schema_ok"]:
                job_logger.warning(f"Gates action: {gate_res['action']} for reasons: {gate_res['reasons']}")
                if self.config.get("governance", {}).get("validation", {}).get("fail_on_critical", True):
                    update_status(job_dir, job_id, "completed", outcome="REFER", reasons=gate_res["reasons"], job_logger=job_logger)
                    
                    if self.stage_name != "decision":
                        dummy_decision = {
                            "__contract_name__": "decision",
                            "__contract_version__": "0.1.0",
                            "decision": "REFER",
                            "limit": 0,
                            "rate": 0,
                            "drivers": [f"Halted at {self.stage_name}"] + gate_res["reasons"]
                        }
                        dec_dir = job_dir / "decision_engine"
                        dec_dir.mkdir(parents=True, exist_ok=True)
                        with open(dec_dir / "decision_output.json", "w") as f:
                            json.dump(dummy_decision, f, indent=2)
                            
                        cam_md = f"# Credit Approval Memo (CAM) - REFER\n\n## Decision: REFER\n\n**Halted at Stage:** {self.stage_name}\n\n## Reasons:\n"
                        for r in gate_res["reasons"]:
                            cam_md += f"- {r}\n"
                        
                        with open(dec_dir / "cam.md", "w") as f:
                            f.write(cam_md)
                            
                        try:
                            from intelligence.decision_engine.export import cam_to_docx, cam_to_pdf
                            cam_to_docx(job_dir)
                            cam_to_pdf(job_dir)
                            from governance.validation.validators import validate_decision
                            validate_decision(job_dir)
                        except Exception as e:
                            job_logger.error(f"Error creating fallback decision output: {e}")
                            
                    self.finalize_job(job_dir, job_logger)
                    return # Halt pipeline
                    
            out_paths = [job_dir / out for out in expected_outputs]
            mark_stage(job_dir, self.stage_name, start_time, _now_iso(), out_paths)
            
            if self.next_queue_name:
                self.publish_event(self.next_queue_name, {"job_id": job_id})
            elif self.stage_name == "decision":
                update_status(job_dir, job_id, "completed", outcome="ALLOW", job_logger=job_logger)
                self.finalize_job(job_dir, job_logger)

        except Exception as e:
            logger.error(f"Worker {self.stage_name} error processing message: {e}")
            
    def finalize_job(self, job_dir: Path, job_logger):
        try:
            from governance.provenance.evidence import build_evidence_pack
            from governance.validation.aggregate import aggregate_reports
            from governance.audit.metrics import collect_metrics
            from governance.provenance.provenance import finish_run
            
            job_logger.info("Building evidence pack...")
            build_evidence_pack(job_dir, self.config)
            
            job_logger.info("Aggregating validation reports...")
            try:
                aggregate_reports(job_dir)
            except Exception as e:
                job_logger.error(f"Failed to aggregate reports: {e}")
            
            job_logger.info("Collecting metrics...")
            collect_metrics(job_dir)
            
            finish_run(job_dir, "completed")
            job_logger.info("Job completed successfully.")
        except Exception as e:
            job_logger.error(f"Error finalizing job: {e}")
            try:
                from governance.provenance.provenance import finish_run
                finish_run(job_dir, "failed")
            except:
                pass
            
    def run_stage(self, job_dir: Path, payload: dict, job_logger):
        raise NotImplementedError
        
    def validate_stage(self, job_dir: Path) -> dict:
        raise NotImplementedError
        
    def get_expected_outputs(self) -> list:
        raise NotImplementedError

    def start_loop(self):
        logger.info(f"Starting {self.stage_name} worker listening to {self.queue_name}...")
        while True:
            try:
                if self.redis_client:
                    # Blocking pop from queue, timeout 5s
                    res = self.redis_client.brpop(self.queue_name, timeout=5)
                    if res:
                        _, message = res
                        logger.info(f"Received message on {self.queue_name}")
                        self.process_message(message)
                else:
                    logger.error("Redis client not initialized.")
                    time.sleep(5)
            except Exception as e:
                logger.error(f"Error in {self.stage_name} loop: {e}")
                time.sleep(5)
