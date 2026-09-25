"""Dataset-agnostic Silver image validation helpers."""

from __future__ import annotations

import io

import pandas as pd
from PIL import Image, UnidentifiedImageError

# Defaults only. A dataset with a structurally different image domain (e.g. whole-slide
# scans, which routinely exceed MAX_DIMENSION as normal, not corrupted) overrides these
# via decode_image's/decode_batch's parameters rather than editing these constants.
# See docs/datasets/ for what each onboarded dataset actually uses.
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


def decode_batch(batches, min_dimension: int = MIN_DIMENSION, max_dimension: int = MAX_DIMENSION):
    """Run decode_image over batches of (image_id, source_split, bronze_uri, image_bytes).

    Generator shaped for Spark's `mapInPandas`: takes an iterator of pandas
    DataFrames, yields an iterator of pandas DataFrames. Only needs pandas,
    not pyspark, so it's callable and testable without a Spark session.
    `min_dimension`/`max_dimension` pass straight through to decode_image; a
    caller overriding them typically binds this via functools.partial first
    (mapInPandas calls the function with just the batch iterator).
    """
    for batch in batches:
        results = []
        for image_id, source_split, bronze_uri, image_bytes in zip(
            batch["image_id"], batch["source_split"], batch["bronze_uri"], batch["image_bytes"]
        ):
            outcome = decode_image(bytes(image_bytes), min_dimension=min_dimension, max_dimension=max_dimension)
            results.append(
                {
                    "image_id": image_id,
                    "source_split": source_split,
                    "bronze_uri": bronze_uri,
                    "valid": outcome["valid"],
                    "image_width": outcome.get("width"),
                    "image_height": outcome.get("height"),
                    "image_format": outcome.get("format"),
                    "validation_reason": outcome.get("reason"),
                }
            )
        yield pd.DataFrame(results)
