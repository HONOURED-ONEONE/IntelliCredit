import pytest
from pathlib import Path
from intelligence.decision_engine.export import cam_to_pdf

def test_cam_to_pdf_stability(tmp_path):
    # Setup job directory with cam.md
    job_dir = tmp_path / "job_pdf"
    de_dir = job_dir / "decision_engine"
    de_dir.mkdir(parents=True)
    
    cam_md_content = """# Credit Approval Memo (CAM)
## Decision: APPROVE
**Limit:** ₹1,000,000.00
- Driver: GST vs Bank mismatch detected
- Driver: **High circular trading risk** detected
Notes: Ensure symbols like & < > are escaped & do not break PDF.
"""
    cam_md_path = de_dir / "cam.md"
    with open(cam_md_path, "w", encoding="utf-8") as f:
        f.write(cam_md_content)
        
    pdf_path = cam_to_pdf(job_dir)
    
    assert pdf_path is not None
    assert pdf_path.exists()
    assert pdf_path.stat().st_size > 1000  # Floor size check

def test_cam_to_pdf_no_file(tmp_path):
    job_dir = tmp_path / "job_no_file"
    job_dir.mkdir()
    
    pdf_path = cam_to_pdf(job_dir)
    assert pdf_path is None
