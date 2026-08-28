"""Shared filesystem helpers for notebooks and scripts."""

from __future__ import annotations

import hashlib
import shutil
import zipfile
from pathlib import Path
from typing import Callable, Iterator

IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png"}


def stage_archive_locally(archive_path: Path, local_root: Path, overwrite: bool = False) -> Path:
    """Copy one source archive to local disk and return the staged path."""
    local_root.mkdir(parents=True, exist_ok=True)
    staged_path = local_root / archive_path.name
    if staged_path.exists():
        if overwrite:
            staged_path.unlink()
        elif staged_path.stat().st_size == archive_path.stat().st_size:
            return staged_path
        else:
            raise RuntimeError(f"Local staged archive differs from source: {staged_path}")
    shutil.copy2(archive_path, staged_path)
    return staged_path


def is_image_file(path: Path) -> bool:
    """Return whether a path has a known image file suffix."""
    return path.suffix.lower() in IMAGE_SUFFIXES


def _iter_zip_members(archive_path: Path) -> Iterator[tuple[zipfile.ZipFile, zipfile.ZipInfo, Path]]:
    """Yield (open handle, member, relative_path) for each non-directory zip entry."""
    with zipfile.ZipFile(archive_path) as handle:
        for member in handle.infolist():
            if member.is_dir():
                continue
            yield handle, member, Path(member.filename)


def extract_zip_members(
    archive_path: Path,
    destination_root: Path,
    predicate: Callable[[Path], bool],
) -> list[Path]:
    """Extract zip members accepted by predicate into destination_root."""
    destination_root.mkdir(parents=True, exist_ok=True)
    extracted_paths = []
    for handle, member, relative_path in _iter_zip_members(archive_path):
        if not predicate(relative_path):
            continue
        target_path = destination_root / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with handle.open(member) as source, target_path.open("wb") as target:
            shutil.copyfileobj(source, target)
        extracted_paths.append(target_path)
    return extracted_paths


def count_zip_members_by_suffix(archive_path: Path) -> dict[str, int]:
    """Count non-directory zip members grouped by lowercased file suffix, for diagnostics."""
    suffix_counts: dict[str, int] = {}
    for _handle, _member, relative_path in _iter_zip_members(archive_path):
        suffix = relative_path.suffix.lower()
        suffix_counts[suffix] = suffix_counts.get(suffix, 0) + 1
    return suffix_counts


def iter_image_blob_rows(
    archive_path: Path,
    source_split: str,
    source_archive_uri: str | None = None,
) -> Iterator[dict]:
    """Yield Spark-ready binary image rows directly from a local zip archive."""
    for handle, member, relative_path in _iter_zip_members(archive_path):
        if not is_image_file(relative_path):
            continue
        image_bytes = handle.read(member)
        yield {
            "image_id": relative_path.stem,
            "source_split": source_split,
            "archive_member_path": relative_path.as_posix(),
            "source_archive_uri": source_archive_uri,
            "image_bytes": image_bytes,
            "byte_length": len(image_bytes),
            "source_checksum": hashlib.sha256(image_bytes).hexdigest(),
        }
