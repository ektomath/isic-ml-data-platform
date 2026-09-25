# 007. Don't persist a copy of data until it's unavoidable

Status: accepted. The shard retention point is amended by [009](009-gold-shard-retention-undecided.md).

## Summary

Stream or compute data from an existing source rather than writing a new copy. When a copy is unavoidable, write it as late in the pipeline as possible and treat it as a rebuildable cache. The cost is reading the same archives more than once. This turns what Bronze, Silver and Gold each arrived at separately into a default for future work.

## Context

While designing [ADR 006](006-stream-archives-no-blob-storage.md), every layer ran into the same question and reached the same answer: the copy wasn't needed. Writing it down means the next pipeline stage starts from that question, instead of starting from a copy and justifying it afterwards.

## Decision

Persist something new only when there's a concrete, current reason, never because a later step might want it. This already holds throughout the pipeline:

- **Bronze** reads each archive to compute checksums, then discards the bytes.
- **Silver** validates images by streaming them from the archive. There's no byte table to join.
- **The source archives** are the only permanent copy of image bytes.
- **Gold shard export** is the one place bytes are written again. It's the latest point in the pipeline, and a copy is needed there because training needs actual files, not a Spark session. The shards are fully rebuilt on each export.
- **Within a notebook**, `materialize()` (write a DataFrame to a table and read it back) is only for a result that's both expensive and used more than once. It currently has no callers.

## Consequences

- A new stage should first ask whether it can read from an existing source before writing anything new.
- Each archive is read once each in Bronze, Silver and Gold export, rather than once overall. That's cheap while archives are few and staged on local disk. If repeated reading ever becomes the bottleneck, revisit that trade-off, not the principle.
