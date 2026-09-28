"""Pure-Python Gold shard packing. No Spark/dbutils dependency, unit-tested
locally like data_platform.files/validate/labels/sampling (see
tests/test_shard_export.py).

This lives outside data_platform.spark_io on purpose, even though it's only
ever called from a Gold notebook: it needs no live Spark session at all (no
`spark`/`dbutils` parameter anywhere below), so it doesn't belong in the
"requires Spark" bucket just because of where it's called from. Kept out of
files.py too — `streaming.MDSWriter` (mosaicml-streaming) is a real, heavy
dependency (pulls in torch/torchvision/transformers transitively) that only
Gold shard export needs; folding this in would drag that import into every
consumer of files.py, including plain Bronze ingestion, which never touches
shards at all.

Shards are a derived, fully rebuildable cache, never a second source of truth
for image bytes — see docs/decisions/004-stream-archives-no-blob-storage.md.
Retention (how long an export is kept around) is a separate, currently
undecided question — see docs/decisions/006-gold-shard-retention-undecided.md.
"""

from __future__ import annotations

from collections import defaultdict
from contextlib import ExitStack

from streaming import MDSWriter

from data_platform.files import iter_archive_matches, parse_bronze_uri

GOLD_SHARD_COLUMNS = {"image": "bytes", "label": "str", "image_id": "str", "dataset_key": "str", "group_id": "str"}


def write_gold_shards_for_splits(
    rows_by_split: dict[str, list[dict]],
    staged_path_by_archive_uri: dict,
    shard_dir_by_split: dict[str, str],
    size_limit_bytes: int,
) -> dict[str, dict]:
    """Stream every split's sampled images out of their source archives in one pass and
    write each into its own streaming.MDSWriter shard set.

    A source archive commonly contributes images to more than one split (splits are
    assigned per leakage-control group, not per archive), so this opens one MDSWriter
    per split up front and streams each referenced archive exactly once, routing each
    matched image to its own split's writer as it's read — rather than opening and
    re-scanning the same archive once per split. No image bytes are persisted anywhere
    except the shard files themselves — each image is read from its source archive,
    written straight into its shard, and discarded (see
    docs/decisions/004-stream-archives-no-blob-storage.md).

    `rows_by_split` must be `data_platform.spark_io.load_manifest_rows_for_export`'s
    `{split: [row, ...]}` shape (each row: `image_id`, `dataset_key`, `bronze_uri`,
    `source_checksum`, `label`, `group_id`). `staged_path_by_archive_uri` maps every
    referenced source_archive_uri to its locally staged path (building it, across every
    dataset_key in the release, reusing
    data_platform.files.stage_archives_and_extract_metadata per dataset, is the calling
    notebook's job, same staging step Bronze/Silver already do). `shard_dir_by_split`
    gives each split's own output directory.

    Raises if any manifest row can't be found in its source archive, or if a found
    image's freshly-computed checksum doesn't match `source_checksum` already recorded
    on the manifest row — unlike Silver's reject-and-continue handling of a normal
    per-row data-quality issue, either case means an archive changed or was corrupted
    after this manifest was published, which breaks the manifest's reproducibility
    guarantee outright, not routine data variance. See
    docs/decisions/005-immutable-source-archives-checksum-verified.md.
    """
    candidates_by_archive_uri: dict[str, dict[str, dict]] = defaultdict(dict)
    for split, rows in rows_by_split.items():
        for row in rows:
            source_archive_uri, archive_member_path = parse_bronze_uri(row["bronze_uri"])
            candidates_by_archive_uri[source_archive_uri][archive_member_path] = {**row, "split": split}

    sample_counts: dict[str, int] = defaultdict(int)
    checksum_mismatches: list[dict] = []
    missing_candidates: list[dict] = []

    with ExitStack() as writer_stack:
        writer_by_split = {
            split: writer_stack.enter_context(
                MDSWriter(
                    out=shard_dir_by_split[split], columns=GOLD_SHARD_COLUMNS, size_limit=size_limit_bytes, exist_ok=True
                )
            )
            for split in rows_by_split
        }
        for candidate_row, archive_row in iter_archive_matches(
            candidates_by_archive_uri, staged_path_by_archive_uri, checksum_mismatches, missing_candidates
        ):
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

    if checksum_mismatches:
        formatted_mismatches = [
            {
                "image_id": mismatch["candidate"]["image_id"],
                "archive_member_path": mismatch["archive_member_path"],
                "expected_checksum": mismatch["expected_checksum"],
                "actual_checksum": mismatch["actual_checksum"],
            }
            for mismatch in checksum_mismatches
        ]
        raise RuntimeError(
            f"{len(checksum_mismatches)} image(s) no longer match the checksum recorded on "
            f"their manifest row -- the source archive changed after this manifest was "
            f"published, which breaks its reproducibility guarantee. First few: "
            f"{formatted_mismatches[:5]}. Source archives must never change after ingestion; a "
            f"genuine source update needs a new source_version, fresh ingestion, and a new Gold "
            f"release, not an in-place archive edit. See "
            f"docs/decisions/005-immutable-source-archives-checksum-verified.md."
        )

    if missing_candidates:
        missing_image_ids = [candidate["image_id"] for candidate in missing_candidates]
        raise RuntimeError(
            f"{len(missing_image_ids)} manifest row(s) missing from their source archive, "
            f"first few: {missing_image_ids[:5]}. A published gold.manifest_rows release "
            "should always be fully resolvable back to its source archives."
        )

    return {
        split: {"sample_count": sample_counts.get(split, 0), "shard_dir": shard_dir_by_split[split]}
        for split in rows_by_split
    }
