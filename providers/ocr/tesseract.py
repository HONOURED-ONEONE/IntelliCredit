import shutil
import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from PIL import Image
except ImportError:
    Image = None

try:
    import pytesseract
except ImportError:
    pytesseract = None

def available() -> bool:
    """Check if tesseract OCR is available and dependencies are installed."""
    if Image is None or pytesseract is None:
        return False
    return shutil.which('tesseract') is not None

def image_from_pdf_page(pdf_path: str, page_index: int) -> Optional['Image.Image']:
    """Render a PDF page to a PIL Image using pdfplumber."""
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            if page_index < 0 or page_index >= len(pdf.pages):
                return None
            page = pdf.pages[page_index]
            return page.to_image(resolution=300).original
    except Exception as e:
        logger.warning(f"Failed to render page {page_index} of {pdf_path}: {e}")
        return None

def ocr_image(img: 'Image.Image') -> str:
    """Run OCR on the given image using pytesseract."""
    if not available():
        logger.warning("OCR requested but tesseract/dependencies not available.")
        return ""
    try:
        text = pytesseract.image_to_string(img)
        return text.strip()
    except Exception as e:
        logger.warning(f"OCR failed: {e}")
        return ""
