"""Dataset-agnostic Silver image validation helpers."""

from __future__ import annotations

import io

from PIL import Image, ImageStat, UnidentifiedImageError

MIN_DIMENSION = 50
MAX_DIMENSION = 15000
UNIFORM_COLOR_STDDEV_THRESHOLD = 5.0


def decode_image(image_bytes: bytes) -> dict:
    """Decode image bytes and report dimensions/format, or why decoding/validation failed."""
    try:
        with Image.open(io.BytesIO(image_bytes)) as image:
            image.load()
            width, height = image.size
            image_format = image.format
            stddev = ImageStat.Stat(image.convert("L")).stddev[0]
    except (UnidentifiedImageError, OSError, ValueError) as error:
        return {"valid": False, "reason": str(error)}

    if width < MIN_DIMENSION or height < MIN_DIMENSION:
        return {"valid": False, "reason": f"dimensions below minimum {MIN_DIMENSION}px: {width}x{height}"}

    if width > MAX_DIMENSION or height > MAX_DIMENSION:
        return {"valid": False, "reason": f"dimensions above maximum {MAX_DIMENSION}px: {width}x{height}"}

    if stddev < UNIFORM_COLOR_STDDEV_THRESHOLD:
        return {"valid": False, "reason": f"near-uniform color (stddev {stddev:.2f})"}

    return {"valid": True, "width": width, "height": height, "format": image_format}
