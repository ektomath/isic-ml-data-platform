# 008. Source archives are immutable after ingestion, and this is checked, not just assumed

Status: accepted

## Context

Since `docs/decisions/006-stream-archives-no-blob-storage.md`, no layer stores image bytes —
Silver validation and Gold shard export both resolve `bronze_uri` (`archive:<source_archive_uri>#<archive_member_path>`)
back to the landing-volume archive and re-read the bytes from there, every time. This only
produces reproducible results if the archive's content at that path never changes after Bronze
first ingested it. If it did change — a re-uploaded archive, a corrected file dropped in place,
anything that touches the landing volume after the fact — every existing reference to that image
(a Silver `image_inventory` row, a published `gold.manifest_rows` release, a shard already
exported from it) would silently start resolving to different bytes than whatever was actually
validated, labeled, or decided upon when it was created. Nothing in the pipeline would notice.
That's a direct hit on reproducibility (`docs/architecture.md`'s Reproducibility rules already
state source images are immutable), and it fails silently rather than loudly, which is the worse
failure mode.

The pipeline already had the exact tool needed to catch this and simply wasn't using it:
`source_checksum` is computed once at Bronze ingestion time and carried through, unchanged, on
every downstream record that references an image — Bronze's `source_metadata`, Silver's
`image_inventory`, and `gold.manifest_rows`. Both Silver validation and Gold shard export
already re-read and re-hash each image's bytes while streaming it (that's just what
`iter_archive_image_rows` does) — the freshly computed hash was being thrown away instead of
compared against the one already on record.

## Decision

Source archives must never change after Bronze ingestion. A genuine source update (a corrected
file, a newer release) goes through a new `source_version` and a fresh ingestion run, never an
in-place edit to an archive already in the landing volume — the same discipline already implied
by "Original source image bytes are immutable" in `docs/architecture.md`, now given a mechanism
that actually enforces it instead of only stating it.

Enforcement, at the two points where bytes get re-read from an archive after Bronze first
recorded their checksum:

- **Silver validation (`validate_images`)** compares each streamed candidate's freshly computed
  checksum against the `source_checksum` already carried on `label_valid_df` (from Bronze). Any
  mismatch **raises immediately, failing the whole run** — it does not route the affected rows
  through `silver.rejected_records` the way a normal per-image validation failure does, because
  a checksum mismatch isn't per-row data variance; it's a systemic integrity problem (the same
  posture already used elsewhere for a systemic violation, e.g. `assert_controlled_vocabularies`).
- **Gold shard export (`write_gold_shards_for_splits`)** compares each streamed image's freshly
  computed checksum against `source_checksum` already carried on the `gold.manifest_rows` row.
  This is the check that matters most for reproducibility specifically, since shard export can
  run an arbitrary amount of time after the manifest it's exporting was published — it's the
  point closest to "someone is about to train on this, is it still what was promised." Any
  mismatch raises immediately, same reasoning as Silver.

Both checks are effectively free: the checksum was already being computed on every streamed
image for other reasons (it's how `iter_archive_image_rows` works); this only adds the
comparison, not a new archive read or a new hashing pass.

## Consequences

- A changed archive now surfaces as a loud, immediate failure at the next Silver or Gold-export
  run that touches it, rather than silently propagating different pixel content into whatever's
  downstream. That's the intended trade — a failed run that has to be investigated is strictly
  better than a manifest that quietly stopped meaning what it says it means.
- This doesn't (and can't) catch a change that happens *between* the two check points and is
  then also reverted before the next one runs — it's not a continuous integrity monitor, just a
  check at the two moments bytes actually get re-read. A dedicated periodic audit (re-stream
  every archive and diff against Bronze's index on some schedule, independent of whether Silver
  or Gold happen to run) would close that gap; not built, since nothing currently needs it and it
  isn't free — see `docs/decisions/007-defer-disk-writes-until-unavoidable.md`'s "don't build for
  a hypothetical" reasoning applied here to a hypothetical monitoring need instead of storage.
- Both raises are all-or-nothing for the run they occur in (fail the whole Silver validation or
  the whole shard-export split, not just the affected image) — deliberate, since a mismatch means
  something is wrong with the archive itself, not with one image, and partial output built next
  to a known integrity problem would be worse than no output.
