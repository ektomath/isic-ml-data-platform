# 006. How long Gold shard exports are kept is undecided

Status: accepted. Covers the shard exports described in [004](004-stream-archives-no-blob-storage.md).

## Summary

Shard exports are a rebuildable cache, but how long to keep them is deliberately left open until there's real usage to base it on. Nothing deletes shards automatically. The cost is that exports accumulate until someone deletes them.

## Context

Shards can always be rebuilt from a published manifest and the source archives, so deleting them is safe. The obvious approach is to delete them on a schedule, through a `retention_days` config field or a cloud storage lifecycle policy. But how often training reuses a release, and what a rebuild really costs, isn't known yet, so any schedule would be a guess.

## Decision

- Retention is neither "short-lived" nor "permanent" by default; it hasn't been chosen.
- There's no retention setting, and the export notebook runs no cleanup step.
- No lifecycle policy is planned.

## Consequences

- Shard exports accumulate under `gold/<dataset_version>/shards/` until deleted by hand. That's accepted until it costs something noticeable.
- Revisit with real usage. If rebuilds are cheap and frequent, use a short retention window. If one export is reused heavily over a long time, treat it more like a permanent, versioned artifact.
