# 011. Baseline training: PyTorch ResNet-18, lightweight MLflow, on-demand registry sync

Status: accepted.

## Summary

[ADR 010](010-pin-data-and-preprocessing-per-training-run.md) set the design for pinned training runs but left the concrete choices until real training code existed. This record makes them: a pretrained PyTorch ResNet-18 as the baseline, the client-only `mlflow-skinny` package, one shared MLflow experiment, and a registry sync split so that Spark code and MLflow code never import each other. Each choice favors the simplest option that meets ADR 010.

## Context

The first training script needed a framework, config file shapes, and a concrete way to get MLflow runs into `gold.training_run_registry`, all usable both locally and on Databricks.

## Decision

- **PyTorch and torchvision, ResNet-18** with ImageNet weights, its final layer replaced to match the run's labels. Both libraries were already installed as dependencies of `mosaicml-streaming`, and are now pinned directly (`torch==2.13.0`, `torchvision==0.28.0`).
- **Class order is pinned in config.** `label_values` in the training-run config fixes which index each class gets, instead of deriving it from the order data happens to be read.
- **`mlflow-skinny` instead of full `mlflow`.** The project only talks to the Databricks-hosted tracking server and never runs its own, so the server components aren't needed.
- **One shared MLflow experiment**, overridable per run. Runs are tagged with `dataset_version`, `preprocessing_version` and `training_run_name`, which is enough to filter and compare them.
- **The registry sync is split in two.** `ml.registry_sync` reads runs from MLflow without Spark, and `data_platform.spark_io.write_training_run_registry_rows` writes them with Spark without importing MLflow. The training notebook runs the sync on demand.
- **The shard location is explicit.** Training uses `--shards-root` if given, and otherwise the path the export notebook writes to. That default works on Databricks and fails with a clear error locally, rather than guessing the environment.

## Alternatives considered

- **Full `mlflow`.** It adds a web server stack this project never runs.
- **One experiment per release.** The tags already separate releases, and separate experiments would make cross-release comparison harder.
- **Automatic environment detection for the shard path.** It saves a flag, but it's the kind of hidden behavior that makes failures confusing.

## Consequences

- Architecture and hyperparameters live directly in the training-run config, not in a separate versioned file. Revisit that if a second architecture is added and hyperparameter sets need to be reused across runs.
- A run that trains without the sync cell running afterwards stays out of the registry until the next sync.
- `pretrained: true` needs outbound network access to download the weights. That hasn't been confirmed against a real Databricks workspace's network policy.
