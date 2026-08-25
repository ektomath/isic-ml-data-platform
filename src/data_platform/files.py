"""Shared filesystem helpers for notebooks and scripts."""

from __future__ import annotations

import hashlib
import tarfile
import zipfile
from pathlib import Path


def extract_archive(archive_path: Path, destination_root: Path) -> Path:
    """Extract a zip or tar archive into a destination folder."""
    destination_root.mkdir(parents=True, exist_ok=True)
    suffixes = [suffix.lower() for suffix in archive_path.suffixes]
    if archive_path.suffix.lower() == ".zip":
        with zipfile.ZipFile(archive_path) as handle:
            handle.extractall(destination_root)
        return destination_root
    if ".tar" in suffixes or archive_path.suffix.lower() in {".tgz", ".gz"}:
        with tarfile.open(archive_path) as handle:
            handle.extractall(destination_root)
        return destination_root
    raise RuntimeError(f"Unsupported archive format: {archive_path.name}")


def sha256_file(path: Path) -> str:
    """Calculate a SHA-256 checksum for a file."""
    chunk_size = 8 * 1024 * 1024
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
