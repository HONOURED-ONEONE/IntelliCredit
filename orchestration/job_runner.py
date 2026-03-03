import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from loguru import logger
import yaml

def load_config() -> dict:
    project_root = Path(__file__).resolve().parent.parent
    config_path = project_root / "config" / "base.yaml"
    if not config_path.exists():
        return {"paths": {"output_root": "outputs/jobs"}}
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

async def run_job_async(job_id: str, payload: dict):
    project_root = Path(__file__).resolve().parent.parent
    config = load_config()
    output_root = config.get("paths", {}).get("output_root", "outputs/jobs")
    job_dir = project_root / output_root / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    
    status_file = job_dir / "status.json"
    
    from governance.audit.metrics import setup_job_logger, collect_metrics
    from governance.provenance.provenance import start_run, mark_stage, finish_run
    from governance.provenance.evidence import build_evidence_pack
    from governance.validation import validators
    from governance.guardrails.policies import apply_gates
    
    job_logger = setup_job_logger(job_dir, job_id, config)
    
    try:
        current_status = {}
        if status_file.exists():
            with open(status_file, "r") as f:
                current_status = json.load(f)
            if current_status.get("stage") == "completed":
                job_logger.info("Job already completed. Skipping.")
                return

        def update_status(stage: str, outcome: str = "PENDING", reasons: list = None):
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
            job_logger.info(f"Status updated to: {stage} (outcome: {outcome})")

        if not current_status:
            update_status("started")
            start_run(job_dir, config, payload)
            job_logger.info("Run started and provenance initialized.")
            
        stages = ["ingestor", "research", "primary", "decision"]
        
        from intelligence.ingestor import ingestor
        from intelligence.research import research_agent
        from intelligence.primary import primary_agent
        from intelligence.decision_engine import decision_engine
        
        module_map = {
            "ingestor": (ingestor, validators.validate_ingestor, ["ingestor/facts.jsonl"]),
            "research": (research_agent, validators.validate_research, ["research/research_findings.jsonl"]),
            "primary": (primary_agent, validators.validate_primary, ["primary/risk_arguments.jsonl"]),
            "decision": (decision_engine, validators.validate_decision, ["decision_engine/decision_output.json"])
        }

        halt_pipeline = False

        for stage in stages:
            if halt_pipeline:
                break
                
            mod, val_func, expected_outputs = module_map[stage]
            val_report_path = job_dir / f"{stage}_validation_report.json"
            
            # Check idempotency
            can_skip = False
            if val_report_path.exists():
                with open(val_report_path, "r", encoding="utf-8") as f:
                    rep = json.load(f)
                    if rep.get("schema_ok", False):
                        outputs_exist = all((job_dir / out).exists() for out in expected_outputs)
                        if outputs_exist:
                            can_skip = True
                            
            if can_skip:
                job_logger.info(f"Stage {stage} outputs and valid report exist. Resume-skip.")
                continue

            update_status(stage)
            job_logger.info(f"Running {stage}...")
            
            start_time = _now_iso()
            
            # Invoke module
            try:
                mod.run(job_dir, config, payload)
                
                # Augment generated files with non-breaking metadata
                contracts_version = config.get("governance", {}).get("contracts_version", "0.1.0")
                for out_file in expected_outputs:
                    out_path = job_dir / out_file
                    if out_path.exists():
                        if out_path.suffix == ".jsonl":
                            from data_layer.contracts.utils import read_jsonl
                            items = read_jsonl(out_path)
                            for item in items:
                                item["schema_version"] = contracts_version
                                item["source"] = f"{stage}_agent"
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
                                data["source"] = f"{stage}_agent"
                                with open(out_path, "w", encoding="utf-8") as f:
                                    json.dump(data, f, indent=2)

            except Exception as e:
                job_logger.error(f"Error in {stage}: {e}")
                
            # Validate
            val_report = val_func(job_dir)
            job_logger.info(f"{stage} validation ok: {val_report['schema_ok']}")
            
            # Apply gates
            gate_res = apply_gates(job_dir, config)
            if gate_res["action"] != "ALLOW" or not val_report["schema_ok"]:
                job_logger.warning(f"Gates action: {gate_res['action']} for reasons: {gate_res['reasons']}")
                if config.get("governance", {}).get("validation", {}).get("fail_on_critical", True):
                    halt_pipeline = True
                    update_status("completed", outcome="REFER", reasons=gate_res["reasons"])
                    # If failed before decision, we must write a dummy decision output
                    if stage != "decision":
                        dummy_decision = {
                            "__contract_name__": "decision",
                            "__contract_version__": "0.1.0",
                            "decision": "REFER",
                            "limit": 0,
                            "rate": 0,
                            "drivers": [f"Halted at {stage}"] + gate_res["reasons"]
                        }
                        dec_dir = job_dir / "decision_engine"
                        dec_dir.mkdir(parents=True, exist_ok=True)
                        with open(dec_dir / "decision_output.json", "w") as f:
                            json.dump(dummy_decision, f, indent=2)
                            
                        # Generate CAM with REFER rationale
                        cam_md = f"# Credit Approval Memo (CAM) - REFER\n\n## Decision: REFER\n\n**Halted at Stage:** {stage}\n\n## Reasons:\n"
                        for r in gate_res["reasons"]:
                            cam_md += f"- {r}\n"
                        
                        with open(dec_dir / "cam.md", "w") as f:
                            f.write(cam_md)
                            
                        from intelligence.decision_engine.export import cam_to_docx, cam_to_pdf
                        cam_to_docx(job_dir)
                        cam_to_pdf(job_dir)
                        
                        validators.validate_decision(job_dir)
                    
            out_paths = [job_dir / out for out in expected_outputs]
            mark_stage(job_dir, stage, start_time, _now_iso(), out_paths)
            
            await asyncio.sleep(0.1)

        if not halt_pipeline:
            update_status("completed", outcome="ALLOW")
            
        job_logger.info("Building evidence pack...")
        build_evidence_pack(job_dir, config)
        
        job_logger.info("Collecting metrics...")
        collect_metrics(job_dir)
        
        finish_run(job_dir, "completed")
        job_logger.info("Job completed successfully.")
        
    except Exception as e:
        job_logger.error(f"Job failed: {e}")
        try:
            from governance.provenance.provenance import finish_run
            finish_run(job_dir, "failed")
        except:
            pass
    finally:
        logger.remove()
