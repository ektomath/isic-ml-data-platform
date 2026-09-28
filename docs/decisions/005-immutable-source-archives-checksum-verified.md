# 005. Source archives are immutable after ingestion, and that's checked

Status: accepted.

## Summary

Every later step re-reads images from the source archives, so an archive changed after ingestion would silently change what every existing record points to. The rule is that archives never change after ingestion, and it's enforced: Silver validation and Gold shard export compare each image's checksum with the one Bronze recorded, and fail the whole run on any mismatch. The check is effectively free, because both steps already hash every image as they read it.

## Context

Since [ADR 004](004-stream-archives-no-blob-storage.md), no layer stores image bytes. A Silver row, a published training release or an exported shard is only reproducible if the archive still holds the same bytes it did at ingestion. If someone re-uploaded an archive or replaced a file in place, those records would quietly point to different images, and nothing would notice.

Bronze already computes a SHA-256 checksum for every image, and it's carried on every downstream record. Silver and Gold export already re-hash each image as they stream it. That new hash just wasn't being compared.

## Decision

- **Archives never change after ingestion.** A corrected file or new release is ingested as a new `source_version`, not edited in place.
- **Silver validation** compares each image's fresh checksum with the one from Bronze.
- **Gold shard export** compares each image's fresh checksum with the one on its manifest row. This one matters most, because export can run long after the release was published, right before someone trains on it.
- **A mismatch fails the whole run.** It isn't recorded as a normal per-image rejection, because it means the archive itself changed, not that one image is bad.

## Alternatives considered

- **Trust that archives don't change.** That's free, but the failure would be silent.
- **A scheduled integrity audit** that re-reads every archive regardless of pipeline runs. It would catch changes sooner, but it isn't free and nothing needs it yet.

## Consequences

- A changed archive causes a loud failure at the next Silver or export run that touches it, instead of quietly changing downstream data.
- The check only runs when bytes are read, so a change made and reverted between two runs goes unnoticed.
