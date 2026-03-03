import pytest
from unittest.mock import patch, MagicMock
from providers.ocr.tesseract import available, ocr_image
from providers.ocr.cleanup import cleanup_image
from governance.validation.validators import validate_ingestor
from governance.provenance.evidence import build_evidence_pack
import json
import os

def test_ocr_available_mocked():
    with patch('providers.ocr.tesseract.shutil.which') as mock_which:
        mock_which.return_value = '/usr/bin/tesseract'
        # Since Image or pytesseract might be None in testing environment if not installed,
        # we just test the logic safely.
        import providers.ocr.tesseract as tess
        original_img = tess.Image
        original_pyt = tess.pytesseract
        tess.Image = MagicMock()
        tess.pytesseract = MagicMock()
        
        assert tess.available() == True
        
        tess.Image = original_img
        tess.pytesseract = original_pyt

def test_cleanup_image_disabled():
    mock_img = MagicMock()
    res = cleanup_image(mock_img, enabled=False)
    assert res == mock_img

def test_validate_ingestor_normalization(tmp_path):
    job_dir = tmp_path / "job_1"
    ingestor_dir = job_dir / "ingestor"
    ingestor_dir.mkdir(parents=True)
    
    facts = [
        {"field": "total_gst_sales", "value": "₹1.5 Cr", "period": "2023Q3", "page": 1, "evidence_snippet": "test"},
        {"field": "total_bank_inflow", "value": "15,000,000", "period": "2024-02", "page": 1, "evidence_snippet": "test"}
    ]
    facts_path = ingestor_dir / "facts.jsonl"
    with open(facts_path, "w") as f:
        for fact in facts:
            f.write(json.dumps(fact) + "
")
            
    # Write empty signals.json
    with open(ingestor_dir / "signals.json", "w") as f:
        f.write("{}")

    report = validate_ingestor(job_dir)
    
    # Check that issues list contains info/warn for normalizations
    issue_codes = [issue["code"] for issue in report["issues"]]
    assert "PERIOD_NORMALIZED" in issue_codes
    assert "CURRENCY_DIFFERENCE" in issue_codes or "CURRENCY_NORMALIZED" in issue_codes
    
    # verify file was updated
    with open(facts_path, "r") as f:
        updated_facts = [json.loads(line) for line in f]
        
    assert updated_facts[0]["value"] == 15000000.0
    assert updated_facts[0]["period"] == "2023-Q3"

def test_evidence_anchors(tmp_path):
    job_dir = tmp_path / "job_1"
    ingestor_dir = job_dir / "ingestor"
    ingestor_dir.mkdir(parents=True)
    
    facts = [
        {"field": "total_gst_sales", "value": 100, "page": 2, "file": "test.pdf", "evidence_snippet": "snippet1"}
    ]
    facts_path = ingestor_dir / "facts.jsonl"
    with open(facts_path, "w") as f:
        for fact in facts:
            f.write(json.dumps(fact) + "
")

    build_evidence_pack(job_dir, {})
    
    pack_dir = job_dir / "evidence_pack"
    manifest_path = pack_dir / "evidence_manifest.json"
    assert manifest_path.exists()
    
    with open(manifest_path, "r") as f:
        manifest = json.load(f)
        
    facts_manifest = [m for m in manifest if m["contract"] == "facts"]
    assert len(facts_manifest) == 1
    assert "anchors" in facts_manifest[0]
    assert facts_manifest[0]["anchors"] == [{"file": "test.pdf", "page": 2}]
