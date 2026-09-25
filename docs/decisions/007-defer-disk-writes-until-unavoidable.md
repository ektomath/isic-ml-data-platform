# 007. Defer writing to disk until it's unavoidable

Status: accepted (one claim below, about Gold shard retention specifically, amended by [009](009-gold-shard-retention-undecided.md))

## Context

`docs/decisions/006-stream-archives-no-blob-storage.md` made one specific call: stop storing
image bytes in Bronze/Silver tables. Working through that redesign surfaced the same question
repeatedly at every layer — Bronze, Silver, and Gold each independently arrived at "stream this
from an existing source instead of writing a new copy of it," not because a rule said to, but
because writing the copy kept turning out to be unnecessary once actually examined. That's worth
naming as its own decision so future work defaults to asking the question, rather than each new
pipeline stage re-deriving the same answer from scratch — or worse, defaulting to "materialize
it, that's simpler" and only getting pulled back later.

## Decision

Prefer streaming or on-demand computation over persisting a new copy of data. When persisting
*is* unavoidable, persist as late in the pipeline as possible, and treat what's persisted as
ephemeral and rebuildable rather than a new source of truth — unless there's a concrete, current
reason it needs to be permanent. Never persist something because a future step *might* want it
that way; that's the same "don't build for a hypothetical" discipline already applied elsewhere
in this project (the deleted zero-logic per-dataset label modules, the deferred incremental
Gold-sync tool), applied here to storage instead of code.

This is already load-bearing in the codebase, not aspirational:

- **Bronze never retains image bytes.** `write_image_index_table` streams each archive to
  compute `byte_length`/`source_checksum` and discards the bytes immediately — it never writes
  them anywhere, not even transiently to a scratch location (`docs/decisions/006-...md`).
- **Silver streams instead of joining a persisted copy.** `validate_images` reads each candidate
  image directly from its source archive, decodes it, and discards the bytes — there is no
  Bronze byte table left to join in the first place, by construction.
- **`materialize()` is used sparingly, not routinely** (it currently has no callers at all). It exists specifically for the case where
  skipping it would mean redoing real work — a DataFrame that's both expensive to compute *and*
  feeds more than one downstream action. A cheap or single-consumer DataFrame is never
  materialized just because it's convenient. This is the same principle applied inside a single
  notebook run, not just across pipeline stages.
- **The landing-volume archives are the one permanent copy of image bytes.** Nothing upstream of
  Bronze re-copies them, and nothing downstream persists a second copy until Gold shard export —
  the latest point in the whole pipeline, and the one point where a real, external constraint
  (a training job needs actual files, not a Spark session) makes persisting unavoidable.
- **Even that unavoidable write is treated as a derived cache, not a new source of truth.** Gold
  shards are fully rebuilt on every export run (`MDSWriter(..., exist_ok=True)`, no
  incremental-sync machinery) — persisting late didn't become an excuse to treat the result as
  authoritative. (This bullet originally also claimed shards carry a retention policy rather than
  being kept indefinitely; that specific claim is retracted by
  [ADR 009](009-gold-shard-retention-undecided.md) — retention is an open question, not decided.)

## Consequences

- Adding a new pipeline stage means asking "can this be computed/streamed from an existing
  source instead of writing a new copy" before reaching for a Volume write or a table column
  that holds derived/duplicated bytes — not after, once the copy already exists and now has to
  be justified or torn out.
- This trades some repeated compute (the same archive gets streamed once each in Bronze, Silver,
  and Gold export, rather than once ever) for avoiding storage duplication and its cost. That
  trade only holds because archives are few, already locally staged before each pass, and
  streaming reads are cheap compared to the per-object storage-operation cost this project has
  already been burned by once (the README's "Lessons learned" section). If archive count or size ever grows
  enough that repeated streaming itself becomes the bottleneck, that's a reason to revisit this
  specific trade — not a reason to abandon the general principle.
- A future stage that does need to persist something should be able to point at a concrete,
  current reason (an external consumer that genuinely needs a file, not a Spark session; a
  DataFrame proven to feed multiple downstream actions) — "it might be useful later" doesn't
  qualify, same bar as everywhere else in this project.
