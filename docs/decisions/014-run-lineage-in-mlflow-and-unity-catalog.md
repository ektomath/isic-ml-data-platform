# 014. Record training lineage in MLflow and Unity Catalog, not a custom registry table

Status: accepted. Replaces the registry table in [010](010-pin-data-and-preprocessing-per-training-run.md) and the registry sync in [011](011-baseline-training-framework-and-registry-sync.md).

## Summary

Training runs used to be copied from MLflow into a `gold.training_run_registry` Delta table by a sync step someone had to run by hand. That table duplicated what MLflow already records, could fall behind it, and isn't how Databricks projects usually track lineage. It's removed. Each run now records its Gold manifest release and the shard files it read as MLflow dataset inputs, and the trained model is registered in Unity Catalog with a signature. The cost is that "which models used this release" is answered through MLflow, not with a plain SQL join against a project table.

## Context

MLflow already stores every run's parameters, metrics, tags (dataset version, preprocessing version, git commit) and artifacts. The registry table's only advantage was SQL access next to the Gold tables. In exchange it added a reader, a Spark writer, tests, a table definition and a notebook cell, and it was only as fresh as the last manual sync.

Databricks' own building blocks cover the same need: MLflow dataset inputs record what a run trained on, and Unity Catalog gives registered models the same permissions, audit logs and lineage as tables.

## Decision

- **Remove `gold.training_run_registry`**, the sync code (`ml.registry_sync`, `write_training_run_registry_rows`) and the sync cell in notebook 40.
- **Log the training data as MLflow inputs.** Every run records two metadata-only datasets: the Gold manifest table for its `dataset_version`, and the shard location it read (a Unity Catalog Volume path on Databricks, a local path otherwise). Nothing is loaded; they only record where the data came from.
- **Register models in Unity Catalog with a signature.** Unity Catalog refuses a model without declared input and output shapes, so training infers one from the trained model (a batch of 3 × `image_size` × `image_size` images in, one score per class out).

## Alternatives considered

- **Keep the table, syncing it automatically after training on Databricks.** Fresher, but still a second copy of MLflow's data to maintain, and local runs would still lag.
- **Query MLflow system tables with SQL.** That would restore SQL access without a copy, but it's a platform feature this project hasn't verified.

## Consequences

- There's one record of each run, in MLflow, and nothing to keep in sync.
- Whether a Volume path input appears in Unity Catalog's lineage graph, and not only on the MLflow run, hasn't been checked on a real workspace. The input is on the MLflow run either way.
- Workspaces that already ran notebook 00 still have an empty `gold.training_run_registry` table. It can be dropped by hand.
