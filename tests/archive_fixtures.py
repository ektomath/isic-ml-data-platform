"""Shared real-fixture helpers for tests that build small zip archives / images on the fly
(tmp_path, in-memory -- never files checked into tests/fixtures/, matching this project's
existing fixture convention). Used by tests/test_shard_export.py and tests/test_ml_*.py.
"""

from __future__ import annotations

import hashlib
import io
import zipfile

from PIL import Image


def checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_archive(tmp_path, members: dict[str, bytes]):
    archive_path = tmp_path / "archive.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return archive_path


def jpeg_bytes(color: tuple[int, int, int], size: int = 32) -> bytes:
    image = Image.new("RGB", (size, size), color=color)
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG")
    return buffer.getvalue()
