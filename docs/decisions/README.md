# Architecture decision records

Each record explains one non-obvious design decision: the context, what was decided, what else was considered, and what it costs. They describe the design as it is now. Where an earlier approach was tried and replaced, the record says what it was and why it changed.

If you only read two, read 004 and 007.

| ADR | Decision |
|---|---|
| [001](001-preprocessing-at-runtime.md) | Resizing and normalization happen at training time; stored data stays unprocessed |
| [002](002-silver-label-columns-not-map.md) | Labels are named columns on one shared Silver table |
| [003](003-cross-dataset-leakage-not-checked.md) | Patient/lesion overlap between datasets isn't checked; exact duplicate images are |
| [004](004-stream-archives-no-blob-storage.md) | Stream images from the source archives; store image bytes nowhere else |
| [005](005-immutable-source-archives-checksum-verified.md) | Source archives are immutable, and every layer re-verifies checksums |
| [006](006-gold-shard-retention-undecided.md) | How long Gold shard exports are kept is deliberately undecided |
| [007](007-pin-data-and-preprocessing-per-training-run.md) | Each training run pins one dataset version and one preprocessing recipe, and records its data in MLflow and Unity Catalog |
| [008](008-baseline-model-resnet18.md) | Baseline model is a pretrained PyTorch ResNet-18 |
| [009](009-pin-metadata-feature-config-per-training-run.md) | Metadata feature engineering is pinned per training run too |
| [010](010-record-git-commit-on-releases-and-runs.md) | Gold releases and training runs record their Git commit, and refuse to run without one |
