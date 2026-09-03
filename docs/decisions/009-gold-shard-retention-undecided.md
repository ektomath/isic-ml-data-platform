# 009. Gold shard retention is an open question, not a decision

Status: accepted

Amends the retention-specific claims in [006](006-stream-archives-no-blob-storage.md) and
[007](007-defer-disk-writes-until-unavoidable.md) — everything else in both stands unchanged.

## Context

ADR 006 introduced Gold shard export and characterized the output as "an ephemeral, fully
rebuildable cache, not a permanent artifact" — pairing that with a `retention_days` config field,
an on-demand `remove_expired_exports` utility, and a stated intent to eventually enforce
autodeletion via a cloud-storage lifecycle policy. ADR 007 repeated the same framing as an
example of persisting-late-not-persisting-permanently.

That bundled two different claims together:

1. Shards are always **derived** — fully reproducible from an already-published manifest plus
   the source archives, never a second source of truth for correctness. Still true, not in
   question.
2. Shards **should be short-lived and actively deleted on a schedule.** This was asserted before
   there was any real signal for how shards will actually be used — how often training reruns
   against the same `dataset_version`, how much a rebuild actually costs in practice, whether a
   longer-lived (or even effectively permanent) shard export turns out to be more useful once
   real usage exists. Committing to an auto-delete policy this early means optimizing for a
   guess, not for anything observed.

## Decision

Retract claim 2 specifically. Retention is genuinely undecided until there's real usage data —
not "short-lived by default," not "permanent by default," just not yet chosen. Concretely:

- `config/gold/exports/<name>.yaml` no longer carries a `retention_days` field — there is no
  current retention policy to configure.
- `notebooks/31_export_gold_shards.ipynb` no longer runs a cleanup step by default.
- `data_platform.spark_io.remove_expired_exports` stays in the codebase as a general-purpose,
  available utility (delete `<dataset_version>` shard directories older than a given age) — it's
  simply not wired into the default export flow, since calling it implies a retention decision
  that hasn't been made. Use it directly, with an explicit day count, whenever that decision does
  get made.
- No cloud-storage lifecycle policy is planned or implied right now. If one gets set up later,
  that's a decision to make at that point, with real usage in hand — not something to pre-commit
  to today.
- Claim 1 is untouched: shards are still always safe to delete and regenerate from the manifest
  and source archives (`MDSWriter(..., exist_ok=True)` already covers a full, deterministic
  rebuild), and are still never where image bytes need to be recovered from. That property holds
  regardless of how long any given export is actually kept around, so it doesn't need to be
  re-decided here.

## Consequences

- Shard exports now accumulate under `gold/<dataset_version>/shards/` until someone manually
  deletes them — there is currently no mechanism, automatic or otherwise, limiting how much
  storage they consume over time. That's an accepted, explicit tradeoff for keeping the option
  open, not an oversight — revisit once it's actually costing something noticeable.
- Revisit this once there's real signal, in either direction: if shards turn out to be cheap and
  frequent to rebuild, a short retention window (or none, if the storage cost turns out to be
  immaterial) makes sense; if a shard export turns out to get reused heavily across many training
  runs over a long stretch, it may be worth treating closer to a permanent, versioned artifact —
  at which point the incremental-sync/content-hash-diffing machinery that the original
  Parquet-export design had (dropped specifically because shards were assumed ephemeral, see
  ADR 006's Decision) becomes worth reconsidering too. Neither direction is assumed here; this
  ADR exists specifically to avoid having prematurely picked one.
