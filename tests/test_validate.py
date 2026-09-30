import io

from PIL import Image, ImageDraw

from data_platform import validate
from data_platform.validate import decode_image


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

