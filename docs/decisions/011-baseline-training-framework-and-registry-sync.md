# 011. Baseline training framework, config shapes, and registry sync mechanism

Status: accepted

## Context

[ADR 010](010-pin-data-and-preprocessing-per-training-run.md) designed
`config/gold/training_runs/<name>.yaml`, `gold.training_run_registry`, and the MLflow bridge
between local and Databricks training, but deliberately deferred every concrete detail —
framework, model architecture, config file fields, and the exact registry sync mechanism —
"until real training code exists to validate the design against." This ADR is that
implementation, and picks those specifics.

## Decision

- **PyTorch + torchvision, ResNet-18** (`ResNet18_Weights.IMAGENET1K_V1`, `fc` replaced for the
  run's label count) as the baseline architecture — matches the original pre-implementation plan
  doc's ML-001 task, and reuses `torch`/`torchvision`, already transitive dependencies of
  `mosaicml-streaming==0.13.0`. Both are pinned as direct dependencies now that real code imports
  them, at the exact versions already resolved transitively (`torch==2.13.0`,
  `torchvision==0.28.0`) — pinning doesn't change what's installed, it just makes the dependency
  explicit, same reasoning already applied to `mosaicml-streaming` itself.
- **`config/preprocessing/<name>.yaml` and `config/gold/training_runs/<name>.yaml` field shapes**
  are as implemented in `src/ml/preprocessing.py` and `src/ml/training_run.py` — see
  `docs/data_contract.md`. `label_values` is pinned in the training-run config, not derived from
  a shard scan, so the class-index mapping is itself a reproducible, versioned fact rather than
  an artifact of scan order.
- **`mlflow-skinny`, not full `mlflow`.** This project only ever talks to the Databricks-hosted
  tracking server over REST (ADR 010 point 3) and never runs its own tracking server, so the full
  package's Flask/gunicorn/sqlalchemy server stack is dead weight — the same "don't drag in more
  than the consumer needs" reasoning that split `shard_export.py` out of `files.py`.
- **One shared, project-wide MLflow experiment** (`ml.mlflow_utils.DEFAULT_MLFLOW_EXPERIMENT`,
  overridable per training-run config), not one experiment per release — runs need to be
  comparable across `dataset_version`/`preprocessing_version` pairs, which is exactly what
  pinning identity per run is for; the `dataset_version`/`preprocessing_version`/
  `training_run_name` tags already give that filtering axis within one experiment, so a second
  experiment-per-release axis would be redundant.
- **Registry sync is split the same way `shard_export.py` is already split from `spark_io.py`**:
  `ml.registry_sync.list_training_run_rows` queries MLflow (needs `mlflow`, no Spark) and returns
  plain dicts; `data_platform.spark_io.write_training_run_registry_rows` does the Spark
  `MERGE INTO` write (needs Spark, never imports `mlflow`) — keeps every existing Bronze/Silver
  notebook's `spark_io` import surface unchanged. Called explicitly from
  `notebooks/40_train_baseline_classifier.ipynb`, on demand — never scheduled, matching
  `remove_expired_exports`'s precedent ([ADR 009](009-gold-shard-retention-undecided.md)).
- **Shard-root resolution is explicit, no environment auto-detection**:
  `ml.train.resolve_shards_root` takes an explicit `--shards-root` override; if omitted, it
  defaults to the same `<storage_root>/gold/<dataset_version>/shards` path
  `31_export_gold_shards.ipynb` already writes to. That default either resolves (Databricks,
  mounted Volume) or fails fast with a clear error (local, requiring the already-documented
  manual `databricks fs cp -r` step in `docs/data_contract.md` — not rebuilt here).

## Consequences

- A model-architecture or hyperparameter-config version was called out in ADR 010 as a plausible
  future addition to the pinned pairing — not added here; `config/gold/training_runs/<name>.yaml`
  already carries `architecture`/`batch_size`/etc. directly (not a separate versioned file) since
  there's exactly one architecture and no reuse-across-runs pressure yet. Revisit if a second
  architecture is ever tried and the same hyperparameter set needs to pair with multiple
  `dataset_version`/`preprocessing_version` combinations.
- `gold.training_run_registry` can only ever reflect MLflow runs that were actually synced —
  running `notebooks/40_train_baseline_classifier.ipynb`'s training cell without also running its sync
  cell leaves a real MLflow run invisible to the registry until someone runs the sync cell later.
  Accepted deliberately, same as every other "exists, not automatic" utility in this project;
  revisit if that gap ever causes a real problem.
- `pretrained: true` requires outbound network access (to download ImageNet weights) on whatever
  compute runs training, both locally and on a Databricks cluster — not verified against the
  actual target cluster's network policy as part of this ADR; confirm before relying on it for a
  real run in a locked-down workspace.
