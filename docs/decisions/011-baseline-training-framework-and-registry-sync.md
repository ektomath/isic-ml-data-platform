# 011. Baseline model: pretrained PyTorch ResNet-18

Status: accepted. This record also originally split a registry sync between MLflow and Spark code; [ADR 014](014-run-lineage-in-mlflow-and-unity-catalog.md) removed that registry.

## Summary

[ADR 010](010-pin-data-and-preprocessing-per-training-run.md) set the design for pinned training runs but left the model choice until real training code existed. The baseline is an ImageNet-pretrained ResNet-18 in PyTorch and torchvision, with its final layer replaced to match the run's labels. It's a well-understood, cheap starting point, and the libraries were already installed.

## Context

The first training script needed a model that trains quickly on a small sample, runs the same on a laptop and on Databricks, and gives a sensible reference number before anything more ambitious is tried.

## Decision

- **PyTorch and torchvision, ResNet-18**, starting from ImageNet weights with the final layer sized to the run's `label_values`. Both libraries were already dependencies of `mosaicml-streaming`, and are now pinned directly.
- **Architecture and hyperparameters live in the training-run config**, not in a separate versioned file, since there's only one architecture so far.

## Alternatives considered

- **A larger or custom architecture.** Not justified before a first baseline run exists.
- **Training from scratch.** Needs far more data and compute than a 200-image sample release offers.

## Consequences

- Revisit the config layout if a second architecture is added and hyperparameter sets need to be reused across runs.
- `pretrained: true` needs outbound network access to download the weights. That hasn't been confirmed against a real Databricks workspace's network policy.
