import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from workers.worker_base import WorkerBase
from intelligence.decision_engine import decision_engine
from governance.validation.validators import validate_decision

class DecisionWorker(WorkerBase):
    def __init__(self):
        super().__init__(
            queue_name="DECISION_REQUESTED",
            next_queue_name=None, # Decision is the last stage
            stage_name="decision"
        )
        
    def run_stage(self, job_dir: Path, payload: dict, job_logger):
        decision_engine.run(job_dir, self.config, payload)
        
    def validate_stage(self, job_dir: Path) -> dict:
        return validate_decision(job_dir)
        
    def get_expected_outputs(self) -> list:
        return ["decision_engine/decision_output.json"]

if __name__ == "__main__":
    worker = DecisionWorker()
    worker.start_loop()