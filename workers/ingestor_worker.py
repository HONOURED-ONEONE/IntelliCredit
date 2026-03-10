import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from workers.worker_base import WorkerBase
from intelligence.ingestor import ingestor
from governance.validation.validators import validate_ingestor

class IngestorWorker(WorkerBase):
    def __init__(self):
        super().__init__(
            queue_name="INGESTION_REQUESTED",
            next_queue_name="RESEARCH_REQUESTED",
            stage_name="ingestor"
        )
        
    def run_stage(self, job_dir: Path, payload: dict, job_logger):
        ingestor.run(job_dir, self.config, payload)
        
    def validate_stage(self, job_dir: Path) -> dict:
        return validate_ingestor(job_dir)
        
    def get_expected_outputs(self) -> list:
        return ["ingestor/facts.jsonl"]

if __name__ == "__main__":
    worker = IngestorWorker()
    worker.start_loop()