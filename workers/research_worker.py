import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from workers.worker_base import WorkerBase
from intelligence.research import research_agent
from governance.validation.validators import validate_research

class ResearchWorker(WorkerBase):
    def __init__(self):
        super().__init__(
            queue_name="RESEARCH_REQUESTED",
            next_queue_name="PRIMARY_INSIGHTS_REQUESTED",
            stage_name="research"
        )
        
    def run_stage(self, job_dir: Path, payload: dict, job_logger):
        research_agent.run(job_dir, self.config, payload)
        
    def validate_stage(self, job_dir: Path) -> dict:
        return validate_research(job_dir)
        
    def get_expected_outputs(self) -> list:
        return ["research/research_findings.jsonl"]

if __name__ == "__main__":
    worker = ResearchWorker()
    worker.start_loop()