"""Dataset-agnostic Silver image validation helpers."""

from __future__ import annotations

import io

from PIL import Image, UnidentifiedImageError

# Defaults. A dataset whose images are normally outside these limits passes its own to
# decode_image; see docs/datasets/.
MIN_DIMENSION = 50
MAX_DIMENSION = 15000


def decode_image(image_bytes: bytes, min_dimension: int = MIN_DIMENSION, max_dimension: int = MAX_DIMENSION) -> dict:
    """Decode image bytes and report dimensions/format, or why decoding/validation failed."""
    try:
        with Image.open(io.BytesIO(image_bytes)) as image:
            image.load()
            width, height = image.size
            image_format = image.format
    except (UnidentifiedImageError, OSError, ValueError) as error:
        return {"valid": False, "reason": str(error)}

    if width < min_dimension or height < min_dimension:
        return {"valid": False, "reason": f"dimensions below minimum {min_dimension}px: {width}x{height}"}

    if width > max_dimension or height > max_dimension:
        return {"valid": False, "reason": f"dimensions above maximum {max_dimension}px: {width}x{height}"}

    return {"valid": True, "width": width, "height": height, "format": image_format}

