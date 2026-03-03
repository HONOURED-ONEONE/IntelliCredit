import pytest
import json
from pathlib import Path
from governance.provenance.evidence import build_evidence_pack

def test_evidence_page_images(tmp_path):
    job_dir = tmp_path / "job_123"
    job_dir.mkdir()
    
    pdfs_dir = job_dir / "inputs" / "pdfs"
    pdfs_dir.mkdir(parents=True)
    pdf_path = pdfs_dir / "test.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 mock") # Not a real pdf, pdfplumber will fail gracefully

    cfg = {
        "governance": {
            "evidence": {
                "store_page_images": True
            }
        }
    }
    
    # This won't actually create images since pdfplumber will fail on mock bytes, 
    # but we can verify it doesn't crash and respects the config structure.
    build_evidence_pack(job_dir, cfg)
    
    pack_dir = job_dir / "evidence_pack"
    assert pack_dir.exists()
    
    manifest_file = pack_dir / "evidence_manifest.json"
    assert manifest_file.exists()
    
    cfg_off = {
        "governance": {
            "evidence": {
                "store_page_images": False
            }
        }
    }
    build_evidence_pack(job_dir, cfg_off)
    # Manifest overwritten
    assert manifest_file.exists()
