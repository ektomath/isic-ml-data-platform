"""Shared filesystem helpers for notebooks and scripts."""

from __future__ import annotations

import hashlib
import shutil
import tarfile
import zipfile
from pathlib import Path
from typing import Callable

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


def is_image_file(path: Path) -> bool:
    """Return whether a path has a known image file suffix."""
    return path.suffix.lower() in IMAGE_SUFFIXES


def list_zip_member_paths(archive_path: Path) -> list[Path]:
    """List non-directory member paths in a zip archive."""
    with zipfile.ZipFile(archive_path) as handle:
        return [Path(member.filename) for member in handle.infolist() if not member.is_dir()]


def extract_zip_members(
    archive_path: Path,
    destination_root: Path,
    predicate: Callable[[Path], bool],
) -> list[Path]:
    """Extract zip members accepted by predicate into destination_root."""
    destination_root.mkdir(parents=True, exist_ok=True)
    extracted_paths = []
    with zipfile.ZipFile(archive_path) as handle:
        for member in handle.infolist():
            if member.is_dir():
                continue
            relative_path = Path(member.filename)
            if not predicate(relative_path):
                continue
            target_path = destination_root / relative_path
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with handle.open(member) as source, target_path.open("wb") as target:
                shutil.copyfileobj(source, target)
            extracted_paths.append(target_path)
    return extracted_paths


def count_zip_image_members(archive_path: Path) -> int:
    """Count image files in a zip archive."""
    return sum(1 for path in list_zip_member_paths(archive_path) if is_image_file(path))


def extract_zip_images(archive_path: Path, destination_root: Path) -> list[Path]:
    """Extract image files from a zip archive into destination_root."""
    return extract_zip_members(archive_path, destination_root, is_image_file)


def build_image_blob_rows(
    archive_path: Path,
    source_split: str,
    source_archive_uri: str | None = None,
) -> list[dict]:
    """Build Spark-ready binary image rows directly from a local zip archive."""
    rows = []
    with zipfile.ZipFile(archive_path) as handle:
        for member in handle.infolist():
            if member.is_dir():
                continue
            member_path = Path(member.filename)
            if not is_image_file(member_path):
                continue
            image_bytes = handle.read(member)
            rows.append(
                {
                    "image_id": member_path.stem,
                    "source_split": source_split,
                    "archive_member_path": member_path.as_posix(),
                    "source_archive_uri": source_archive_uri,
                    "image_bytes": image_bytes,
                    "byte_length": len(image_bytes),
                    "source_checksum": hashlib.sha256(image_bytes).hexdigest(),
                }
            )
    return rows


def materialize_zip_images_and_metadata(
    archive_path: Path,
    image_root: Path,
    metadata_root: Path | None = None,
    metadata_filename: str | None = None,
    overwrite: bool = False,
) -> dict:
    """Extract zip images idempotently and optionally extract one metadata file."""
    expected_image_count = count_zip_image_members(archive_path)
    if expected_image_count == 0:
        raise RuntimeError(f"No image files found in archive: {archive_path}")

    existing_images = list_files_by_suffix(image_root, IMAGE_SUFFIXES)

    if overwrite and image_root.exists():
        shutil.rmtree(image_root)

    if not overwrite and len(existing_images) == expected_image_count:
        image_status = "existing"
        image_count = len(existing_images)
    elif not overwrite and existing_images:
        raise RuntimeError(
            f"Found partial image extraction: {len(existing_images)} existing files, "
            f"expected {expected_image_count}"
        )
    else:
        image_root.mkdir(parents=True, exist_ok=True)
        extracted_images = extract_zip_images(archive_path, image_root)
        image_count = len(extracted_images)
        if image_count != expected_image_count:
            raise RuntimeError(f"Extracted {image_count} images, expected {expected_image_count}")
        image_status = "re-extracted" if overwrite else "extracted"

    metadata_path = None
    if metadata_filename is not None:
        if metadata_root is None:
            raise ValueError("metadata_root is required when metadata_filename is provided")
        if overwrite and metadata_root.exists():
            shutil.rmtree(metadata_root)
        extracted_metadata_files = extract_zip_members(
            archive_path,
            metadata_root,
            lambda path: path.name == metadata_filename,
        )
        if len(extracted_metadata_files) != 1:
            raise RuntimeError(
                f"Expected to extract exactly one {metadata_filename}, "
                f"extracted {len(extracted_metadata_files)}"
            )
        metadata_path = extracted_metadata_files[0]

    return {
        "expected_image_count": expected_image_count,
        "image_count": image_count,
        "image_status": image_status,
        "metadata_path": metadata_path,
    }


def list_files_by_suffix(root: Path, suffixes: set[str]) -> list[Path]:
    """List files under root whose suffix matches one of suffixes."""
    if not root.exists():
        return []
    normalized_suffixes = {suffix.lower() for suffix in suffixes}
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in normalized_suffixes
    )


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
