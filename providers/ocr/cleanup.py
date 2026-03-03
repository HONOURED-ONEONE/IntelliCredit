import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from PIL import Image, ImageEnhance, ImageFilter
except ImportError:
    Image = None

def cleanup_image(img: 'Image.Image', enabled: bool = False) -> 'Image.Image':
    """Apply Chandra-lite cleanup to an image for better OCR if enabled."""
    if not enabled or Image is None:
        return img
    try:
        # Simple denoise and contrast enhancement
        img = img.convert('L')
        img = img.filter(ImageFilter.MedianFilter())
        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(1.5)
        return img
    except Exception as e:
        logger.warning(f"Image cleanup failed: {e}")
        return img
