"""Packs a Gold release's images into MosaicML shards. Needs no Spark session, so it's tested
locally (tests/test_shard_export.py). Kept apart from files.py because mosaicml-streaming is a heavy
dependency that only the shard export needs.

Shards are a rebuildable cache, never a copy of the image bytes to rely on (ADR 004).
"""

from __future__ import annotations

import shutil
from collections import defaultdict
from contextlib import ExitStack
from pathlib import Path

from streaming import MDSWriter

from data_platform.files import ArchiveMatches, parse_bronze_uri, raise_on_checksum_mismatches

PARTIAL_SUFFIX = ".partial"

GOLD_SHARD_COLUMNS = {"image": "bytes", "label": "str", "image_id": "str", "dataset_key": "str", "group_id": "str"}


def write_gold_shards_for_splits(
    rows_by_split: dict[str, list[dict]],
    staged_path_by_archive_uri: dict,
    shard_dir_by_split: dict[str, str],
    size_limit_bytes: int,
) -> dict[str, dict]:
    """Stream every split's images out of their source archives and write each split's shards with
    MDSWriter.

    Each referenced archive is read once, with every image routed to its split's writer, since one
    archive usually feeds several splits. Image bytes go straight from the archive into the shard
    and nowhere else (ADR 004).

    `rows_by_split` is load_manifest_rows_for_export's output: {split: [row, ...]}, each row with
    image_id, dataset_key, bronze_uri, source_checksum, label and group_id.
    `staged_path_by_archive_uri` maps each source_archive_uri to its locally staged copy, and
    `shard_dir_by_split` gives each split's output directory.

    Shards are written to `<split dir>.partial` and moved into place only after every split is
    written and verified. On failure the partial folders are deleted and the previous export is left
    as it was, so a split folder with an index.json is always complete.

    Raises if a manifest row isn't in its archive or its checksum no longer matches: the archive
    changed after the release was published, which breaks the release (ADR 005). Silver, by
    contrast, rejects such rows and carries on.
    """
    candidates_by_archive_uri: dict[str, dict[str, dict]] = defaultdict(dict)
    for split, rows in rows_by_split.items():
        for row in rows:
            source_archive_uri, archive_member_path = parse_bronze_uri(row["bronze_uri"])
            candidates_by_archive_uri[source_archive_uri][archive_member_path] = {**row, "split": split}

    sample_counts: dict[str, int] = defaultdict(int)
    matches = ArchiveMatches(candidates_by_archive_uri, staged_path_by_archive_uri)
    partial_dir_by_split = {split: shard_dir_by_split[split] + PARTIAL_SUFFIX for split in rows_by_split}

    try:
        for partial_dir in partial_dir_by_split.values():
            shutil.rmtree(partial_dir, ignore_errors=True)

        with ExitStack() as writer_stack:
            writer_by_split = {
                split: writer_stack.enter_context(
                    MDSWriter(out=partial_dir, columns=GOLD_SHARD_COLUMNS, size_limit=size_limit_bytes, exist_ok=True)
                )
                for split, partial_dir in partial_dir_by_split.items()
            }
            for candidate_row, archive_row in matches:
                writer_by_split[candidate_row["split"]].write(
                    {
                        "image": archive_row["image_bytes"],
                        "label": candidate_row["label"],
                        "image_id": candidate_row["image_id"],
                        "dataset_key": candidate_row["dataset_key"],
                        "group_id": candidate_row["group_id"],
                    }
                )
                sample_counts[candidate_row["split"]] += 1

        raise_on_checksum_mismatches(matches.checksum_mismatches, "the checksum recorded on their manifest row")
        if matches.missing:
            missing_image_ids = [candidate["image_id"] for candidate in matches.missing]
            raise RuntimeError(
                f"{len(missing_image_ids)} manifest row(s) missing from their source archive, "
                f"first few: {missing_image_ids[:5]}. A published gold.manifest_rows release "
                "should always be fully resolvable back to its source archives."
            )
    except BaseException:
        for partial_dir in partial_dir_by_split.values():
            shutil.rmtree(partial_dir, ignore_errors=True)
        raise

    # Every split verified: swap each finished export into place.
    for split, partial_dir in partial_dir_by_split.items():
        final_dir = shard_dir_by_split[split]
        shutil.rmtree(final_dir, ignore_errors=True)
        Path(final_dir).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(partial_dir, final_dir)

    return {
        split: {"sample_count": sample_counts.get(split, 0), "shard_dir": shard_dir_by_split[split]}
        for split in rows_by_split
    }
