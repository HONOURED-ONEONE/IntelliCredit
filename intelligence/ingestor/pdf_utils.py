from pathlib import Path
from loguru import logger

def extract_text_pages(pdf_path: Path) -> list:
    pages = []
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                pages.append({"page": i + 1, "text": text[:500]})
    except ImportError:
        logger.warning("pdfplumber not available. Skipping PDF text extraction.")
    except Exception as e:
        logger.warning(f"Error reading PDF {pdf_path}: {e}")
    return pages
