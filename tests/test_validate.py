import io

import pandas as pd
from PIL import Image, ImageDraw

from data_platform import validate
from data_platform.validate import decode_batch, decode_image


def _varied_image_bytes(width: int, height: int, image_format: str = "PNG") -> bytes:
    image = Image.new("RGB", (width, height), color=(255, 0, 0))
    draw = ImageDraw.Draw(image)
    draw.rectangle([0, 0, width // 2, height // 2], fill=(0, 255, 0))
    draw.rectangle([width // 2, height // 2, width, height], fill=(0, 0, 255))
    buffer = io.BytesIO()
    image.save(buffer, format=image_format)
    return buffer.getvalue()


def test_decode_image_reports_dimensions_and_format_for_valid_png():
    image_bytes = _varied_image_bytes(64, 64, "PNG")

    assert decode_image(image_bytes) == {"valid": True, "width": 64, "height": 64, "format": "PNG"}


def test_decode_image_reports_jpeg_format():
    image_bytes = _varied_image_bytes(64, 64, "JPEG")

    result = decode_image(image_bytes)

    assert result == {"valid": True, "width": 64, "height": 64, "format": "JPEG"}


def test_decode_image_rejects_corrupt_bytes():
    result = decode_image(b"not an image")

    assert result["valid"] is False
    assert "reason" in result


def test_decode_image_rejects_empty_bytes():
    result = decode_image(b"")

    assert result["valid"] is False
    assert "reason" in result


def test_decode_image_rejects_dimensions_below_minimum():
    image_bytes = _varied_image_bytes(10, 10, "PNG")

    result = decode_image(image_bytes)

    assert result["valid"] is False
    assert "minimum" in result["reason"]


def test_decode_image_rejects_dimensions_above_maximum():
    image_bytes = _varied_image_bytes(64, 64, "PNG")

    result = decode_image(image_bytes, max_dimension=60)

    assert result["valid"] is False
    assert "maximum" in result["reason"]


def test_decode_image_accepts_dimensions_at_minimum_boundary():
    image_bytes = _varied_image_bytes(validate.MIN_DIMENSION, validate.MIN_DIMENSION, "PNG")

    result = decode_image(image_bytes)

    assert result["valid"] is True


def test_decode_image_dimension_overrides_do_not_affect_default_calls():
    image_bytes = _varied_image_bytes(64, 64, "PNG")

    assert decode_image(image_bytes, max_dimension=60)["valid"] is False
    assert decode_image(image_bytes)["valid"] is True


def test_decode_batch_reports_per_row_results_and_carries_passthrough_columns():
    input_df = pd.DataFrame(
        [
            {
                "image_id": "a",
                "source_split": "train",
                "bronze_uri": "table:bronze.x_image_blobs/train/a",
                "image_bytes": _varied_image_bytes(64, 64, "PNG"),
            },
            {
                "image_id": "b",
                "source_split": "test",
                "bronze_uri": "table:bronze.x_image_blobs/test/b",
                "image_bytes": b"not an image",
            },
        ]
    )

    output_df = pd.concat(list(decode_batch(iter([input_df]))), ignore_index=True)

    assert list(output_df["image_id"]) == ["a", "b"]
    assert list(output_df["source_split"]) == ["train", "test"]
    assert list(output_df["bronze_uri"]) == ["table:bronze.x_image_blobs/train/a", "table:bronze.x_image_blobs/test/b"]
    assert list(output_df["valid"]) == [True, False]
    assert output_df.loc[0, "image_width"] == 64
    assert output_df.loc[0, "image_height"] == 64
    assert output_df.loc[0, "image_format"] == "PNG"
    assert output_df.loc[1, "validation_reason"] is not None


def test_decode_batch_passes_dimension_overrides_through_to_decode_image():
    input_df = pd.DataFrame(
        [{"image_id": "a", "source_split": "train", "bronze_uri": "u1", "image_bytes": _varied_image_bytes(64, 64)}]
    )

    output_df = pd.concat(list(decode_batch(iter([input_df]), max_dimension=60)), ignore_index=True)

    assert bool(output_df.loc[0, "valid"]) is False
    assert "maximum" in output_df.loc[0, "validation_reason"]


def test_decode_batch_yields_one_output_frame_per_input_batch():
    batch_1 = pd.DataFrame(
        [{"image_id": "a", "source_split": "train", "bronze_uri": "u1", "image_bytes": _varied_image_bytes(64, 64)}]
    )
    batch_2 = pd.DataFrame(
        [{"image_id": "b", "source_split": "train", "bronze_uri": "u2", "image_bytes": _varied_image_bytes(64, 64)}]
    )

    outputs = list(decode_batch(iter([batch_1, batch_2])))

    assert len(outputs) == 2
    assert list(outputs[0]["image_id"]) == ["a"]
    assert list(outputs[1]["image_id"]) == ["b"]
