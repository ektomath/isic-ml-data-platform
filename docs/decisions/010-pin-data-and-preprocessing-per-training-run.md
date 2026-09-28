# 010. Pin the dataset and preprocessing for every training run

Status: accepted. Implemented by [ADR 011](011-baseline-training-framework-and-registry-sync.md). The registry table in point 3 was later replaced by [ADR 014](014-run-lineage-in-mlflow-and-unity-catalog.md).

## Summary

Because preprocessing is applied at training time ([ADR 001](001-preprocessing-at-runtime.md)), the stored data can't say which preprocessing a model used. Each training run is therefore defined by one config file that pairs exactly one `dataset_version` with one `preprocessing_version`, and training accepts nothing else. Every run logs that pairing, and the data it read, to MLflow. A model's metrics can always be traced to the exact images and preprocessing behind them, without relying on anyone remembering to log it.

## Context

One Gold shard export serves any number of preprocessing experiments, which avoids duplicate storage. But then nothing in the export or the manifest records what a specific model was trained with. The manifest's `preprocessing_version` is only the default at publish time.

## Decision

1. **A training-run config pins the pairing.** `config/gold/training_runs/<name>.yaml` names one `dataset_version` and one `preprocessing_version`, plus the hyperparameters. Training takes only this config's name, never two separate values, so there's no way to train on an unpinned combination. This follows the project's existing pattern of one versioned config file per thing.
2. **MLflow records what happened.** Every run, local or on Databricks, logs hyperparameters, metrics, the model and the pairing to the MLflow server built into the Databricks workspace. A local machine reaches it over REST with a personal access token, and needs no Spark session. Each run also saves the full preprocessing recipe as an artifact, because a recipe file edited in place keeps its name.
3. **The run records its training data.** Each run logs the Gold manifest release and the shard files it read as MLflow dataset inputs, and the model is registered in Unity Catalog. This originally was a separate `gold.training_run_registry` table synced from MLflow; [ADR 014](014-run-lineage-in-mlflow-and-unity-catalog.md) replaced it.
4. **A new Gold release is for a new image selection only.** Shards hold unprocessed bytes, so two releases that differ only in `preprocessing_version` would export identical shards. A new preprocessing experiment is a new training-run config, not a new release.

## Alternatives considered

- **Bake preprocessing into the shard path**, with one export per preprocessing version. This brings back the duplicate storage ADR 001 avoids.
- **Rely on training code to log both values.** Nothing would enforce it.
- **Hash the dataset, preprocessing and resolved config into one model identity.** It's more rigorous, but has more moving parts than a single baseline model justifies. Worth revisiting if the simpler design falls short.

## Consequences

- Trying new preprocessing against the same images costs one new config file, with no new export.
- Local and Databricks training share the same logging path. Running locally only needs an access token.
