# 011. Baseline training: PyTorch ResNet-18, with the registry sync kept apart from Spark

Status: accepted.

## Summary

[ADR 010](010-pin-data-and-preprocessing-per-training-run.md) set the design for pinned training runs but left two concrete choices until real training code existed. The baseline model is a pretrained PyTorch ResNet-18. The step that copies MLflow runs into `gold.training_run_registry` is split so that the MLflow code needs no Spark and the Spark code never imports MLflow. Both favor the simplest option that meets ADR 010 and keeps local training free of Spark.

## Context

Training has to run the same way on a laptop and on Databricks. A laptop has no Spark session, and Databricks notebooks shouldn't pull MLflow into every Bronze and Silver import. The registry table lives in Unity Catalog, so something has to bridge MLflow and Spark.

## Decision

- **PyTorch and torchvision, ResNet-18**, starting from ImageNet weights with the final layer replaced to match the run's labels. Both libraries were already installed as dependencies of `mosaicml-streaming`, and ResNet-18 is a well-understood, cheap baseline.
- **The registry sync is split in two.** `ml.registry_sync` reads runs from MLflow without Spark and returns plain rows. `data_platform.spark_io.write_training_run_registry_rows` writes those rows with Spark and never imports MLflow. The training notebook runs the sync on demand after training.

## Alternatives considered

- **Write the registry directly from training code.** A local run would then need a Spark session just to log one row.
- **Import MLflow in `spark_io`.** Every Bronze and Silver notebook would then depend on MLflow for no reason.
- **A larger or custom architecture.** Not justified before a first baseline run exists.

## Consequences

- A run that trains without the sync cell running afterwards stays out of the registry until the next sync.
- Architecture and hyperparameters live directly in the training-run config. Revisit that if a second architecture is added and hyperparameter sets need to be reused across runs.
- `pretrained: true` needs outbound network access to download the weights. That hasn't been confirmed against a real Databricks workspace's network policy.
