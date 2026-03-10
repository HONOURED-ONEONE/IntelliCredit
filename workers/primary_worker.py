import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from workers.worker_base import WorkerBase
from intelligence.primary import primary_agent
from governance.validation.validators import validate_primary

class PrimaryWorker(WorkerBase):
    def __init__(self):
        super().__init__(
            queue_name="PRIMARY_INSIGHTS_REQUESTED",
            next_queue_name="DECISION_REQUESTED",
            stage_name="primary"
        )
        
    def run_stage(self, job_dir: Path, payload: dict, job_logger):
        primary_agent.run(job_dir, self.config, payload)
        
    def validate_stage(self, job_dir: Path) -> dict:
        return validate_primary(job_dir)
        
    def get_expected_outputs(self) -> list:
        return ["primary/risk_arguments.jsonl"]

if __name__ == "__main__":
    worker = PrimaryWorker()
    worker.start_loop()