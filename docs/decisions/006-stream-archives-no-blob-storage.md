# 006. Stream archives directly — no image-blob storage at any layer

Status: accepted (retention framing below amended by [009](009-gold-shard-retention-undecided.md))

Supersedes [002](002-store-bronze-images-as-delta-blobs.md).

## Context

ADR 002 fixed a real, measured cost problem — extracting one Volume object per image cost
roughly $17 and 90 minutes for ~33k ISIC 2019 images, because on cloud object storage operation
count dominates cost, not byte volume (`docs/lessons-learned.md`). Its fix was to stage each
archive locally and write image bytes as rows into a Bronze Delta table
(`bronze.<dataset>_image_blobs`) instead.

That fix was correct for the problem it solved, but it left a second, smaller inefficiency in
place: image bytes now exist in two places — the original source archives (landing Volume,
immutable, checksummable) and the Bronze blob table (a second, growing copy of the same bytes).
Silver's `validate_images` then joined that blob table to decode/validate images, and any
eventual Gold export would have had to join it a third time. None of this data actually needs a
second home: the archives are already the durable, checksummed source of truth, and every layer
that ever needs bytes (Silver validation, Gold shard export) can stream them directly from the
archive instead.

## Decision

- **Bronze never stores image bytes.** `bronze.<dataset>_image_blobs` is replaced by
  `bronze.<dataset>_image_index` — the identical row shape minus the `image_bytes` column
  (`image_id`, `source_split`, `archive_member_path`, `source_archive_uri`, `byte_length`,
  `source_checksum`). Bronze ingestion still streams every archive once to compute each image's
  checksum/byte_length, it just never retains the bytes past that computation
  (`write_image_index_table`, `data_platform.files.iter_archive_image_rows`).
- **Silver never stores or joins image bytes.** `validate_images` streams each label-valid
  candidate's bytes directly from its (locally staged) source archive, decodes/validates via the
  unchanged `data_platform.validate.decode_image`/`decode_batch`, and discards the bytes
  immediately after. No Bronze blob table is joined, because none exists.
- **`bronze_uri` is repointed at the archive, not a table row.** It used to be a synthetic,
  unparseable `table:<table>/<split>/<image_id>` debug string. It's now
  `archive:<source_archive_uri>#<archive_member_path>` — a real, resolvable pointer
  (`data_platform.files.parse_bronze_uri` is its inverse) — since it's now the *only* way any
  downstream layer can relocate an image's bytes. `gold.manifest_rows.bronze_uri` alone is
  enough to find an image's bytes at Gold export time, no extra join back to Bronze needed.
- **Gold's last step packs an already-published manifest's images into ephemeral MosaicML
  shards**, not a permanent table or file. `notebooks/31_export_gold_shards.ipynb` streams each
  split's sampled images out of their source archives (parsing `bronze_uri`) and writes them via
  `streaming.MDSWriter` into `gold/<dataset_version>/shards/<split>/`. Because a shard set is
  built from an already-split manifest, split assignment always happens before sharding, never
  after — consistent with how `gold.manifest_rows.split` already worked before this change.
- **Shards are a fully rebuildable, derived cache, not a permanent second source of truth.**
  Rerunning the export notebook for the same `dataset_version` always fully rewrites every
  split's shard directory (`MDSWriter(..., exist_ok=True)`) — no incremental-sync or
  content-hash-diffing machinery, because a shard set can always be regenerated identically from
  the manifest and source archives.
  > **Amended by [ADR 009](009-gold-shard-retention-undecided.md):** the rest of this bullet, as
  > originally written, additionally claimed shards *should* be short-lived and actively deleted
  > (a `retention_days` config field, an intended cloud-storage lifecycle policy). That specific
  > claim is retracted — retention is an open question, not decided either way. See ADR 009.
- **Images are never individually extracted to a Volume, at any layer, for any reason.** This was
  already true for Bronze under ADR 002; this ADR extends the same rule to Silver and Gold. The
  only files this project ever writes per image are the (few, large, bounded) MDS shard files —
  never a JPEG-per-object write anywhere.

## Consequences

- Removing the Bronze blob table is a real migration, not just new code: ISIC 2019's Bronze and
  Silver had already been run once against real data under the old design (32,413 accepted,
  1,156 rejected, 16,800 leakage-control groups — recorded in `AGENT.md`). Adopting this ADR
  means rerunning both under the new streaming code and dropping the old blob table. The rerun
  doubles as a regression check: label normalization and validation logic didn't change, only
  how bytes are sourced, so the new counts should match the old ones exactly.
- Silver validation now streams archives directly (driver-side, the same access pattern Bronze
  ingestion already uses and has already proven on real Databricks serverless compute) instead of
  a Spark-native join + `mapInPandas`. `data_platform.validate.decode_image`/`decode_batch`
  themselves needed zero changes — only the orchestration around them did.
- A Gold shard export is a real, if bounded, compute cost every time it runs (it re-streams the
  relevant archives), traded against never having to touch the earlier Bronze blob table's
  eventual write-amplification/`MERGE INTO` cost concerns at all, since that table no longer
  exists.
- `mosaicml-streaming` is a new dependency, and its `torch`/`torchvision`/`transformers`/cloud-SDK
  transitive dependencies are pulled in even though no training framework or cloud-remote
  streaming path has been chosen yet for this project — a real, heavier install footprint,
  accepted as the cost of using the library's `MDSWriter`/`StreamingDataset` rather than a
  hand-rolled shard format.
