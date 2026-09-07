"""Baseline classifier training entry point.

Works identically whether invoked as a local CLI/script or from a Databricks notebook cell
(`notebooks/40_train_baseline_classifier.ipynb`) -- the only difference is where `--shards-root` points
(a `databricks fs cp`'d local directory vs a mounted Databricks Volume, see
docs/data_contract.md's Gold shard export section) and how MLflow credentials are resolved
(`ml.mlflow_utils.configure_mlflow_tracking`). See docs/decisions/010 and 011 for the full design.

Local CLI usage:
    python -m ml.train --training-run-name sample-v1-resnet18 --shards-root /local/copied/shards
"""

from __future__ import annotations

import argparse
from pathlib import Path

import mlflow
import mlflow.pytorch
import torch
from torch import nn

from data_platform.dataset_layout import join_storage_path, load_yaml_config
from ml.dataset import build_dataloader, label_to_index_map
from ml.metrics import compute_classification_metrics
from ml.mlflow_utils import configure_mlflow_tracking, current_git_commit
from ml.preprocessing import build_transforms
from ml.training_run import resolve_training_run

DEFAULT_CONFIG_ROOT = Path(__file__).resolve().parents[2] / "config"


def _build_resnet18(num_classes: int, pretrained: bool) -> nn.Module:
    from torchvision.models import ResNet18_Weights, resnet18

    weights = ResNet18_Weights.IMAGENET1K_V1 if pretrained else None
    model = resnet18(weights=weights)
    model.fc = nn.Linear(model.fc.in_features, num_classes)
    return model


# One entry per supported architecture -- a plain dict, matching build_optimizer's if/elif
# below in spirit; add a new architecture by adding a builder function and an entry here.
_ARCHITECTURE_BUILDERS = {"resnet18": _build_resnet18}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the baseline Gold classifier.")
    parser.add_argument("--training-run-name", required=True, help="Name of a config/gold/training_runs/<name>.yaml")
    parser.add_argument("--config-root", default=None, help="Defaults to the repo's config/ directory")
    parser.add_argument("--shards-root", default=None, help="Local directory or Volume path holding train/validation/test shard dirs; defaults to <storage_root>/gold/<dataset_version>/shards")
    parser.add_argument("--mlflow-experiment", default=None, help="Overrides the training-run config's mlflow_experiment")
    parser.add_argument("--mlflow-tracking-uri", default=None, help="Advanced/test override; normally resolved automatically")
    parser.add_argument("--device", default=None, help="Defaults to cuda if available, else cpu")
    parser.add_argument("--num-workers", type=int, default=0)
    return parser.parse_args(argv)


def resolve_shards_root(shards_root: str | None, storage_config: dict, dataset_version: str) -> str:
    """If shards_root is given, use it verbatim -- the local-machine path, pointing at a
    `databricks fs cp -r`'d shard directory (docs/data_contract.md). If omitted, compute the
    same `<storage_root>/gold/<dataset_version>/shards` path
    `notebooks/31_export_gold_shards.ipynb` already writes to -- resolves for free on Databricks
    (the Volume is directly mounted there); off Databricks, this fails fast with a clear
    FileNotFoundError once a dataloader tries to open it, unless --shards-root was passed.
    """
    if shards_root is not None:
        return shards_root
    return join_storage_path(storage_config["storage_root"], f"gold/{dataset_version}/shards")


def build_model(architecture: str, num_classes: int, pretrained: bool) -> nn.Module:
    if architecture not in _ARCHITECTURE_BUILDERS:
        raise ValueError(f"Unsupported architecture {architecture!r}; supported: {sorted(_ARCHITECTURE_BUILDERS)}")
    return _ARCHITECTURE_BUILDERS[architecture](num_classes, pretrained)


def build_optimizer(model: nn.Module, optimizer_name: str, learning_rate: float) -> torch.optim.Optimizer:
    if optimizer_name == "adam":
        return torch.optim.Adam(model.parameters(), lr=learning_rate)
    if optimizer_name == "sgd":
        return torch.optim.SGD(model.parameters(), lr=learning_rate)
    raise ValueError(f"Unsupported optimizer {optimizer_name!r}; use 'adam' or 'sgd'.")


class _LossAccumulator:
    """On-device running mean, shared by train_one_epoch/evaluate -- accumulating on-device and
    syncing to a Python float once (.mean()) rather than once per batch is what avoids a
    host<->device sync every iteration."""

    def __init__(self, device):
        self.total = torch.zeros((), device=device)
        self.count = 0

    def update(self, loss) -> None:
        self.total += loss.detach()
        self.count += 1

    def mean(self) -> float:
        return (self.total / max(self.count, 1)).item()


def _forward_batch(model, images, labels, device, loss_fn=None):
    """Move one batch to device and run the forward pass -- the one step train_one_epoch and
    evaluate share. Returns (outputs, labels_on_device, loss_or_None). `non_blocking=True`
    lets the host->device copy overlap with compute when the DataLoader's tensors are pinned
    (see build_dataloader's `pin_memory=True`) and the target device is CUDA; a harmless no-op
    otherwise (unpinned tensors or a CPU device)."""
    images, labels = images.to(device, non_blocking=True), labels.to(device, non_blocking=True)
    outputs = model(images)
    loss = loss_fn(outputs, labels) if loss_fn is not None else None
    return outputs, labels, loss


def train_one_epoch(model, dataloader, optimizer, loss_fn, device) -> dict:
    model.train()
    loss_accumulator = _LossAccumulator(device)
    for images, labels in dataloader:
        _outputs, _labels, loss = _forward_batch(model, images, labels, device, loss_fn)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        loss_accumulator.update(loss)
    return {"train_loss": loss_accumulator.mean()}


def evaluate(model, dataloader, device, label_values: list[str], loss_fn=None) -> dict:
    """Runs inference over dataloader and returns ml.metrics.compute_classification_metrics'
    output, plus {"loss": ...} if loss_fn is given."""
    model.eval()
    all_preds, all_labels = [], []
    loss_accumulator = _LossAccumulator(device)
    with torch.no_grad():
        for images, labels in dataloader:
            outputs, labels, loss = _forward_batch(model, images, labels, device, loss_fn)
            if loss is not None:
                loss_accumulator.update(loss)
            all_preds.append(outputs.argmax(dim=1))
            all_labels.append(labels)

    # One host sync for predictions/labels, and one for the loss, instead of one per batch.
    preds = torch.cat(all_preds).cpu().tolist()
    labels = torch.cat(all_labels).cpu().tolist()

    metrics = compute_classification_metrics(labels, preds, label_values)
    if loss_fn is not None:
        metrics["loss"] = loss_accumulator.mean()
    return metrics


def run_training(
    training_run_name: str,
    config_root: str | Path | None = None,
    shards_root: str | None = None,
    mlflow_experiment: str | None = None,
    mlflow_tracking_uri: str | None = None,
    device: str | None = None,
    num_workers: int = 0,
) -> str:
    """The reusable orchestrator -- identical whether called from a CLI/local script or a
    Databricks notebook cell (`notebooks/40_train_baseline_classifier.ipynb`'s training cell). Returns
    the MLflow run_id.
    """
    config_root = Path(config_root) if config_root is not None else DEFAULT_CONFIG_ROOT
    spec = resolve_training_run(config_root, training_run_name)
    random_seed = spec.preprocessing_config["random_seed"]
    torch.manual_seed(random_seed)

    storage_config = load_yaml_config(config_root / "storage.yaml")
    base_shards_root = resolve_shards_root(shards_root, storage_config, spec.dataset_version)

    label_to_index = label_to_index_map(spec.label_values)
    train_transform = build_transforms(spec.preprocessing_config, "train")
    eval_transform = build_transforms(spec.preprocessing_config, "eval")

    split_settings = {
        "train": (train_transform, True),
        "validation": (eval_transform, False),
        "test": (eval_transform, False),
    }
    loaders = {
        split: build_dataloader(
            join_storage_path(base_shards_root, split), transform, label_to_index, spec.batch_size, shuffle=shuffle, num_workers=num_workers
        )
        for split, (transform, shuffle) in split_settings.items()
    }
    train_loader, validation_loader, test_loader = loaders["train"], loaders["validation"], loaders["test"]

    resolved_device = device or ("cuda" if torch.cuda.is_available() else "cpu")
    configure_mlflow_tracking(mlflow_experiment or spec.mlflow_experiment, mlflow_tracking_uri)

    model = build_model(spec.architecture, len(spec.label_values), spec.pretrained).to(resolved_device)
    optimizer = build_optimizer(model, spec.optimizer, spec.learning_rate)
    loss_fn = nn.CrossEntropyLoss()

    with mlflow.start_run(
        run_name=training_run_name,
        tags={
            "training_run_name": spec.training_run_name,
            "dataset_version": spec.dataset_version,
            "preprocessing_version": spec.preprocessing_version,
        },
    ) as run:
        mlflow.log_params(
            {
                "architecture": spec.architecture,
                "pretrained": spec.pretrained,
                "batch_size": spec.batch_size,
                "num_epochs": spec.num_epochs,
                "learning_rate": spec.learning_rate,
                "optimizer": spec.optimizer,
                "random_seed": random_seed,
                "git_commit": current_git_commit(),
                "device": resolved_device,
            }
        )

        for epoch in range(spec.num_epochs):
            train_metrics = train_one_epoch(model, train_loader, optimizer, loss_fn, resolved_device)
            validation_metrics = evaluate(model, validation_loader, resolved_device, spec.label_values, loss_fn)
            mlflow.log_metrics(
                {
                    "train_loss": train_metrics["train_loss"],
                    "validation_loss": validation_metrics["loss"],
                    "validation_balanced_accuracy": validation_metrics["balanced_accuracy"],
                },
                step=epoch,
            )

        test_metrics = evaluate(model, test_loader, resolved_device, spec.label_values, loss_fn)
        mlflow.log_metrics(
            {
                "test_balanced_accuracy": test_metrics["balanced_accuracy"],
                **{f"test_recall_{label}": recall for label, recall in test_metrics["per_class_recall"].items()},
            }
        )
        mlflow.log_dict(
            {"confusion_matrix": test_metrics["confusion_matrix"], "label_values": spec.label_values},
            "confusion_matrix.json",
        )
        mlflow.log_text(test_metrics["classification_report"], "classification_report.txt")
        # serialization_format="pickle": mlflow-skinny's default ("pt2") is a traced-graph
        # format requiring a real input_example to trace the model graph through -- pickle
        # needs none of that and is simpler/more reliable for a baseline classifier this size.
        mlflow.pytorch.log_model(model, artifact_path="model", serialization_format="pickle")

        return run.info.run_id


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run_id = run_training(
        training_run_name=args.training_run_name,
        config_root=args.config_root,
        shards_root=args.shards_root,
        mlflow_experiment=args.mlflow_experiment,
        mlflow_tracking_uri=args.mlflow_tracking_uri,
        device=args.device,
        num_workers=args.num_workers,
    )
    print(f"MLflow run_id: {run_id}")


if __name__ == "__main__":
    main()
