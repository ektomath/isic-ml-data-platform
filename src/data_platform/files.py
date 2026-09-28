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


def iter_archive_image_rows(
    archive_path: Path,
    source_split: str,
    source_archive_uri: str | None = None,
    member_predicate: Callable[[Path], bool] | None = None,
) -> Iterator[dict]:
    """Yield one dict per image directly from a local zip archive, bytes included.

    This is the one shared low-level primitive for every archive-streaming
    consumer in this project (Bronze index build, Silver validation, Gold shard
    export) — each decides for itself how long to keep `image_bytes` past its
    own immediate use (Bronze: drops it after hashing; Silver: drops it after
    decode; Gold export: keeps it just long enough to write a shard sample). No
    image bytes are ever written back to a Volume or a table by this function
    itself, and none should be persisted by any of its callers either — see
    docs/decisions/004-stream-archives-no-blob-storage.md.

    `member_predicate`, when given, is checked (by relative in-archive path)
    before bytes are read at all — a caller that only wants a known subset of
    an archive's images (Silver validating a subset that survived label
    normalization; Gold export pulling just a sampled manifest's images out of
    a full source archive) skips reading and checksumming the rest, rather
    than paying for every image just to discard most of them.
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
    """Build the archive:<source_archive_uri>#<archive_member_path> URI used to
    locate an image's bytes — the one canonical, pure-Python definition of this
    format, and the inverse of parse_bronze_uri below.

    `data_platform.spark_io.bronze_image_uri` builds the same string as a
    lazy Spark Column expression instead of calling this directly (it can't —
    Spark Columns are evaluated per-row inside Spark, not by calling a plain
    Python function once), so that one stays a separate implementation. Any
    driver-side Python code reconstructing this string (as opposed to building a
    Spark Column) should call this function rather than retyping the format.
    """
    return f"archive:{source_archive_uri}#{archive_member_path}"


def parse_bronze_uri(bronze_uri: str) -> tuple[str, str]:
    """Recover (source_archive_uri, archive_member_path) from a bronze_uri string.

    The inverse of format_bronze_uri above (and, in Spark-Column form,
    `data_platform.spark_io.bronze_image_uri`) — this is the only way any
    downstream layer (Silver validation, Gold shard export) can locate an
    image's bytes, since no layer stores them. Raises ValueError on anything
    not in that exact shape, since a malformed bronze_uri means the image it
    points at can never be found.
    """
    prefix = "archive:"
    if not bronze_uri.startswith(prefix) or "#" not in bronze_uri:
        raise ValueError(f"Not a resolvable archive bronze_uri: {bronze_uri!r}")
    source_archive_uri, _, archive_member_path = bronze_uri[len(prefix) :].partition("#")
    return source_archive_uri, archive_member_path


def iter_archive_matches(
    candidates_by_archive_uri: dict[str, dict[str, dict]],
    staged_path_by_archive_uri: dict,
    checksum_mismatches: list[dict],
    missing_candidates: list[dict],
) -> Iterator[tuple[dict, dict]]:
    """Yield (candidate_row, archive_row) once per archive member successfully matched
    and checksum-verified, streaming each referenced archive exactly once. This is the
    one shared implementation of the archive-matching and checksum-verification pattern
    used both by Silver validation and Gold shard export -- see
    docs/decisions/005-immutable-source-archives-checksum-verified.md.

    A generator, not a list-returning function, on purpose: don't collect it into a
    list if a candidate's `image_bytes` (on the yielded archive_row) should be dropped
    as soon as it's used (e.g. immediately written to a Gold shard) -- the same
    discipline `iter_archive_image_rows` already documents for its own callers.

    `candidates_by_archive_uri` groups candidate rows (each needing at least
    `source_checksum`) by source_archive_uri and then archive_member_path -- typically
    built by parsing a set of bronze_uri values with `parse_bronze_uri`.
    `staged_path_by_archive_uri` maps each source_archive_uri to its locally staged
    path; an archive_uri missing from this dict contributes only missing candidates,
    never an error.

    Appends to `checksum_mismatches` (dicts: candidate, archive_member_path,
    expected_checksum, actual_checksum) and `missing_candidates` (candidate rows never
    found in their archive) as a side effect during iteration -- read them only after
    fully exhausting this generator. This function only detects mismatches/missing
    rows, it never raises; callers decide what that means for them (raise immediately
    vs. reject-and-continue).
    """
    for source_archive_uri, member_to_candidate in candidates_by_archive_uri.items():
        staged_path = staged_path_by_archive_uri.get(source_archive_uri)
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
                    checksum_mismatches.append(
                        {
                            "candidate": candidate_row,
                            "archive_member_path": archive_row["archive_member_path"],
                            "expected_checksum": candidate_row["source_checksum"],
                            "actual_checksum": archive_row["source_checksum"],
                        }
                    )
                    continue
                yield candidate_row, archive_row

        missing_candidates.extend(member_to_candidate[member] for member in remaining_members)


def check_archives_exist(archives: list[dict]) -> None:
    """Raise FileNotFoundError if any archive's local landing path is missing.

    Assumes archive-based ingestion (each archive dict has `archive_local_path`/
    `archive_dbfs_path`, e.g. from `data_platform.dataset_layout.resolve_archive_paths`) —
    a dataset ingested from an API or another non-archive source has nothing to
    preflight here and doesn't need this function.
    """
    for archive in archives:
        if not archive["archive_local_path"].exists():
            raise FileNotFoundError(f"Missing archive: {archive['archive_dbfs_path']}")
    print("All required source archives are present.")


def stage_archives_and_extract_metadata(
    archives: list[dict], local_stage_root: Path, overwrite: bool = False
) -> list[dict]:
    """Stage each archive locally and extract its single metadata file, setting
    `staged_archive_path`/`metadata_target_path` on each archive dict. Returns the
    same (mutated) archives list.

    Assumes archive-based ingestion where each archive has exactly one metadata
    file matching `metadata_filename` — a dataset ingested from an API or shipping
    metadata some other way needs its own loader, not this function.
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
        print(f"Staged archive locally: {archive['staged_archive_path']}")
        print(f"Copied metadata file into Bronze metadata: {archive['metadata_target_path']}")
    return archives
