"""Shared filesystem helpers for notebooks and scripts."""

from __future__ import annotations

import hashlib
import logging
import shutil
import zipfile
from collections.abc import Callable, Iterator
from pathlib import Path

IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png"}

logger = logging.getLogger(__name__)


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


def iter_archive_image_rows(
    archive_path: Path,
    source_split: str,
    source_archive_uri: str | None = None,
    member_predicate: Callable[[Path], bool] | None = None,
) -> Iterator[dict]:
    """Yield one dict per image in a local zip archive, bytes included.

    The one archive reader behind Bronze indexing, Silver validation and the shard export. Callers
    drop the bytes as soon as they're done with them; nothing is persisted (ADR 004).

    `member_predicate`, when given, is checked on the member path before any bytes are read, so a
    caller that needs only some images skips reading the rest.
    """
    for handle, member, relative_path in _iter_zip_members(archive_path):
        if not is_image_file(relative_path):
            continue
        if member_predicate is not None and not member_predicate(relative_path):
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


def format_bronze_uri(source_archive_uri: str, archive_member_path: str) -> str:
    """Build the `archive:<source_archive_uri>#<archive_member_path>` URI that locates an image's
    bytes. The inverse of parse_bronze_uri; spark_io.bronze_image_uri builds the same string as a
    Spark Column.
    """
    return f"archive:{source_archive_uri}#{archive_member_path}"


def parse_bronze_uri(bronze_uri: str) -> tuple[str, str]:
    """Split a bronze_uri into (source_archive_uri, archive_member_path), which is all that's needed
    to find the image in its archive. Raises ValueError on a malformed URI.
    """
    prefix = "archive:"
    if not bronze_uri.startswith(prefix) or "#" not in bronze_uri:
        raise ValueError(f"Not a resolvable archive bronze_uri: {bronze_uri!r}")
    source_archive_uri, _, archive_member_path = bronze_uri[len(prefix) :].partition("#")
    return source_archive_uri, archive_member_path


class ArchiveMatches:
    """Stream each referenced archive once and yield (candidate_row, archive_row) for every
    candidate found with the checksum it was recorded with. Shared by Silver validation and
    the Gold shard export (docs/decisions/005-immutable-source-archives-checksum-verified.md).

    Loop over it once; afterwards `checksum_mismatches` holds the images whose bytes changed
    (dicts: candidate, archive_member_path, expected_checksum, actual_checksum) and `missing`
    the candidate rows not found in their archive. It never raises: callers decide whether a
    problem fails the run or becomes a rejection. Don't collect the matches into a list if the
    image bytes on each archive_row should be dropped as soon as they're used.

    `candidates_by_archive_uri` maps source_archive_uri -> archive_member_path -> candidate row
    (each needing `source_checksum`), typically built by parsing bronze_uri values with
    `parse_bronze_uri`. `staged_path_by_archive_uri` maps each source_archive_uri to its locally
    staged copy; an archive missing from it contributes only missing candidates.
    """

    def __init__(self, candidates_by_archive_uri: dict[str, dict[str, dict]], staged_path_by_archive_uri: dict):
        self.candidates_by_archive_uri = candidates_by_archive_uri
        self.staged_path_by_archive_uri = staged_path_by_archive_uri
        self.checksum_mismatches: list[dict] = []
        self.missing: list[dict] = []

    def __iter__(self) -> Iterator[tuple[dict, dict]]:
        for source_archive_uri, member_to_candidate in self.candidates_by_archive_uri.items():
            staged_path = self.staged_path_by_archive_uri.get(source_archive_uri)
            remaining_members = set(member_to_candidate)

            if staged_path is not None:
                for archive_row in iter_archive_image_rows(
                    staged_path,
                    source_split="",
                    source_archive_uri=source_archive_uri,
                    member_predicate=lambda path, members=remaining_members: path.as_posix() in members,
                ):
                    remaining_members.discard(archive_row["archive_member_path"])
                    candidate_row = member_to_candidate[archive_row["archive_member_path"]]
                    if archive_row["source_checksum"] != candidate_row["source_checksum"]:
                        self.checksum_mismatches.append(
                            {
                                "candidate": candidate_row,
                                "archive_member_path": archive_row["archive_member_path"],
                                "expected_checksum": candidate_row["source_checksum"],
                                "actual_checksum": archive_row["source_checksum"],
                            }
                        )
                        continue
                    yield candidate_row, archive_row

            self.missing.extend(member_to_candidate[member] for member in remaining_members)


def raise_on_checksum_mismatches(checksum_mismatches: list[dict], expected_source: str) -> None:
    """Raise if any streamed image no longer matches its recorded checksum (as collected by
    ArchiveMatches): the source archive changed after the checksum was recorded, which
    breaks reproducibility for everything built from it
    (docs/decisions/005-immutable-source-archives-checksum-verified.md). `expected_source` names
    where the expected checksum came from, for the error message."""
    if not checksum_mismatches:
        return
    examples = [
        {
            "image_id": mismatch["candidate"]["image_id"],
            "archive_member_path": mismatch["archive_member_path"],
            "expected_checksum": mismatch["expected_checksum"],
            "actual_checksum": mismatch["actual_checksum"],
        }
        for mismatch in checksum_mismatches[:5]
    ]
    raise RuntimeError(
        f"{len(checksum_mismatches)} image(s) no longer match {expected_source}: the source "
        f"archive changed after that, which breaks reproducibility. First few: {examples}. "
        f"Source archives must never change; a genuine update needs a new source_version and a "
        f"fresh ingestion. See docs/decisions/005-immutable-source-archives-checksum-verified.md."
    )


def check_archives_exist(archives: list[dict]) -> None:
    """Raise FileNotFoundError if any archive is missing from the landing Volume."""
    for archive in archives:
        if not archive["archive_local_path"].exists():
            raise FileNotFoundError(f"Missing archive: {archive['archive_dbfs_path']}")
    logger.info("All required source archives are present.")


def stage_archives_and_extract_metadata(
    archives: list[dict], local_stage_root: Path, overwrite: bool = False
) -> list[dict]:
    """Copy each archive to local disk and extract its metadata file, setting `staged_archive_path`
    and `metadata_target_path` on each archive dict. Returns the same list. Expects exactly one
    metadata file per archive, named `metadata_filename`.
    """
    for archive in archives:
        archive["staged_archive_path"] = stage_archive_locally(
            archive_path=archive["archive_local_path"],
            local_root=local_stage_root,
            overwrite=overwrite,
        )
        extracted_metadata_files = extract_zip_members(
            archive_path=archive["staged_archive_path"],
            destination_root=archive["metadata_local_root"],
            predicate=lambda path, metadata_filename=archive["metadata_filename"]: path.name == metadata_filename,
        )
        if len(extracted_metadata_files) != 1:
            raise RuntimeError(
                f"Expected one {archive['metadata_filename']} in {archive['archive_filename']}, "
                f"extracted {len(extracted_metadata_files)}"
            )
        archive["metadata_target_path"] = str(extracted_metadata_files[0])
        logger.info(f"Staged archive locally: {archive['staged_archive_path']}")
        logger.info(f"Copied metadata file into Bronze metadata: {archive['metadata_target_path']}")
    return archives
