# 009. How long Gold shard exports are kept is undecided

Status: accepted. Amends the retention claims in [006](006-stream-archives-no-blob-storage.md) and [007](007-defer-disk-writes-until-unavoidable.md).

## Summary

ADRs 006 and 007 originally said shard exports should be short-lived and deleted on a schedule. That was decided before there was any real usage to base it on, so it's retracted. Retention is now explicitly open: nothing deletes shards automatically, and a cleanup utility exists for when a policy is chosen. The cost is that exports accumulate until someone deletes them.

## Context

The original design made two claims about shards:

1. They're derived: they can always be rebuilt from a published manifest and the source archives. That still holds.
2. They should be deleted on a schedule, through a `retention_days` config field and eventually a cloud storage lifecycle policy. Nobody knew yet how often training would reuse a release or what a rebuild really costs, so this was optimizing for a guess.

## Decision

- Withdraw claim 2. Retention is neither "short-lived" nor "permanent" by default; it hasn't been chosen.
- Export configs no longer have `retention_days`, and the export notebook runs no cleanup step.
- `remove_expired_exports` stays available, to call with an explicit age once a policy is decided.
- No lifecycle policy is planned.

## Consequences

- Shard exports accumulate under `gold/<dataset_version>/shards/` until deleted by hand. That's accepted until it costs something noticeable.
- Revisit with real usage. If rebuilds are cheap and frequent, use a short retention window. If one export is reused heavily over a long time, treat it more like a permanent, versioned artifact.
