# Architecture decision records

Each record explains one non-obvious design decision: the context, what was decided, and what it costs. Superseded records are kept, because how a decision changed is part of the story.

If you only read two, read 006 and 010.

| ADR | Decision | Status |
|---|---|---|
| [001](001-preprocessing-at-runtime.md) | Resizing and normalization happen at training time; stored data stays unprocessed | Accepted |
| [002](002-store-bronze-images-as-delta-blobs.md) | Store Bronze image bytes as Delta rows instead of one file per image | Superseded by 006 |
| [003](003-silver-label-columns-not-map.md) | Labels are named columns on one shared Silver table | Accepted |
| [004](004-cross-dataset-leakage-not-checked.md) | Patient/lesion overlap between datasets isn't checked; exact duplicate images are | Accepted |
| [006](006-stream-archives-no-blob-storage.md) | Stream images from the source archives; store image bytes nowhere else | Accepted |
| [007](007-defer-disk-writes-until-unavoidable.md) | Don't persist a copy of data until it's unavoidable | Accepted |
| [008](008-immutable-source-archives-checksum-verified.md) | Source archives are immutable, and every layer re-verifies checksums | Accepted |
| [009](009-gold-shard-retention-undecided.md) | How long Gold shard exports are kept is deliberately undecided | Accepted |
| [010](010-pin-data-and-preprocessing-per-training-run.md) | Each training run pins one dataset version and one preprocessing version | Accepted |
| [011](011-baseline-training-framework-and-registry-sync.md) | Baseline training framework, config shapes, and registry sync | Accepted |
| [012](012-pin-metadata-feature-config-per-training-run.md) | Metadata feature engineering is pinned per training run too | Accepted |

There is no ADR 005; the number was skipped.
