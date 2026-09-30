"""Baseline classifier training entry point.

Works identically whether invoked as a local CLI/script or from a Databricks notebook cell
(`notebooks/40_train_baseline_classifier.ipynb`) -- the only difference is where `--shards-root` points
(a `databricks fs cp`'d local directory vs a mounted Databricks Volume, see
docs/data_contract.md's Gold shard export section) and how MLflow credentials are resolved
(`ml.mlflow_utils.configure_mlflow_tracking`). See docs/decisions/007 and 008 for the full design.

Local CLI usage:
    python -m ml.train --training-run-name sample-v1-resnet18 --shards-root /local/copied/shards
"""

from __future__ import annotations

import argparse
import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path

import mlflow
import mlflow.pytorch
import torch
from mlflow.data.dataset_source_registry import resolve_dataset_source
from mlflow.data.delta_dataset_source import DeltaDatasetSource
from mlflow.data.meta_dataset import MetaDataset
from mlflow.data.uc_volume_dataset_source import UCVolumeDatasetSource
from mlflow.models import infer_signature
from torch import nn

from data_platform.dataset_layout import join_storage_path, load_yaml_config
from ml.dataset import build_dataloader, label_to_index_map
from ml.metrics import compute_classification_metrics
from data_platform import configure_logging
from data_platform.provenance import resolve_git_commit
from ml.mlflow_utils import configure_mlflow_tracking, configure_model_registry
from ml.preprocessing import build_transforms
from ml.training_run import TrainingRunSpec, resolve_training_run

logger = logging.getLogger(__name__)

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

# Architectures that take image input only. Every architecture is image-only today; a
# multimodal one that consumes metadata features stays out of this set.
_IMAGE_ONLY_ARCHITECTURES = {"resnet18"}


def check_metadata_preprocessing_is_used(architecture: str, metadata_preprocessing_version: str | None) -> None:
    """Raise if a training-run config pins a metadata_preprocessing_version for an image-only
    architecture. The model would never see those features, so recording the version would
    claim a provenance the run doesn't have (docs/decisions/009-pin-metadata-feature-config-per-training-run.md)."""
    if metadata_preprocessing_version is not None and architecture in _IMAGE_ONLY_ARCHITECTURES:
        raise ValueError(
            f"Training-run config sets metadata_preprocessing_version={metadata_preprocessing_version!r}, "
            f"but architecture {architecture!r} uses images only and would ignore it. Remove the field, "
            "or use an architecture that consumes metadata features."
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the baseline Gold classifier.")
    parser.add_argument("--training-run-name", required=True, help="Name of a config/gold/training_runs/<name>.yaml")
    parser.add_argument("--config-root", default=None, help="Defaults to the repo's config/ directory")
    parser.add_argument("--shards-root", default=None, help="Local directory or Volume path holding train/validation/test shard dirs; defaults to <storage_root>/gold/<dataset_version>/shards")
    parser.add_argument("--mlflow-experiment", default=None, help="Overrides the training-run config's mlflow_experiment")
    parser.add_argument("--mlflow-tracking-uri", default=None, help="Advanced/test override; normally resolved automatically")
    parser.add_argument(
        "--registered-model-name",
        default=None,
        help="Overrides the training-run config's registered_model_name (e.g. a Unity Catalog "
        "'catalog.schema.model' name); if neither is set, the model is logged to the run but not registered",
    )
    parser.add_argument("--registry-uri", default=None, help="Advanced/test override for the model registry backend; normally resolved automatically (Unity Catalog)")
    parser.add_argument("--device", default=None, help="Defaults to cuda if available, else cpu")
    parser.add_argument("--num-workers", type=int, default=0)
    return parser.parse_args(argv)


def resolve_shards_root(shards_root: str | None, storage_config: dict, dataset_version: str) -> str:
    """If shards_root is given, use it verbatim -- the local-machine path, pointing at a
    `databricks fs cp -r`'d shard directory (docs/data_contract.md). If omitted, compute the
    same `<storage_root>/gold/<dataset_version>/shards` path
    `notebooks/31_export_gold_shards.ipynb` already writes to -- resolves for free on Databricks
    (the Volume is directly mounted there). Existence is checked separately by
    check_shards_exist, not here -- this function only computes the path.
    """
    if shards_root is not None:
        return shards_root
    return join_storage_path(storage_config["storage_root"], f"gold/{dataset_version}/shards")


def log_training_inputs(dataset_version: str, base_shards_root: str, manifest_table: str) -> None:
    """Record on the active MLflow run which data it trained on: the Gold release's rows in
    manifest_table and the shard files actually read. Metadata only, nothing is loaded. This is
    what links a model back to its Gold release in MLflow and Unity Catalog lineage, in place of
    a separate registry table (docs/decisions/007-pin-data-and-preprocessing-per-training-run.md)."""
    manifest = MetaDataset(
        source=DeltaDatasetSource(
            delta_table_name=manifest_table, delta_table_version=current_delta_table_version(manifest_table)
        ),
        name=f"{manifest_table}@{dataset_version}",
    )
    shards_source = (
        UCVolumeDatasetSource(base_shards_root)
        if base_shards_root.startswith("/Volumes/")
        else resolve_dataset_source(base_shards_root)
    )
    shards = MetaDataset(
        source=shards_source, name=f"shards@{dataset_version}", digest=shard_digest(base_shards_root)
    )
    mlflow.log_input(manifest, context="training")
    mlflow.log_input(shards, context="training")


def shard_digest(base_shards_root: str, splits: tuple[str, ...] = ("train", "validation", "test")) -> str:
    """A fingerprint of the exact shard files a run reads: SHA-256 over every split's
    `index.json`, which lists each shard file with its size and hashes. A later export that
    rewrites the same folder produces a different digest, so a logged path alone can't hide it."""
    digest = hashlib.sha256()
    for split in splits:
        digest.update(split.encode())
        digest.update(Path(join_storage_path(base_shards_root, split), "index.json").read_bytes())
    return digest.hexdigest()[:32]


def current_delta_table_version(table_name: str) -> int | None:
    """The manifest table's current Delta version when a Spark session is active (Databricks),
    so the logged input pins the table's exact state. None locally, where there's no Spark."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    spark = SparkSession.getActiveSession()
    if spark is None:
        return None
    return int(spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").first()["version"])


def tag_registered_model_version(model_name: str, version: str, tags: dict[str, str]) -> None:
    """Copy the run's lineage tags onto its registered model version, so the Unity Catalog model
    page shows them without opening the MLflow run."""
    client = mlflow.MlflowClient()
    for key, value in tags.items():
        client.set_model_version_tag(model_name, version, key, value)


def model_signature(model: nn.Module, image_size: int, device: str):
    """The input/output signature Unity Catalog requires to register a model: a batch of
    3 x image_size x image_size float images in, one score per class out. Inferred from a single
    zero image run through the trained model, so it always matches the model's real shapes."""
    example_input = torch.zeros(1, 3, image_size, image_size)
    model.eval()
    with torch.no_grad():
        example_output = model(example_input.to(device)).cpu().numpy()
    return infer_signature(example_input.numpy(), example_output)


def check_shards_exist(base_shards_root: str, splits: tuple[str, ...] = ("train", "validation", "test")) -> None:
    """Raise FileNotFoundError up front, listing every missing split at once, if any split's
    shard directory doesn't exist yet under base_shards_root -- same "fail loud before the
    expensive part" pattern as data_platform.files.check_archives_exist, rather than letting a
    missing/wrong shards_root surface as a cryptic `RuntimeError: Stream contains no samples`
    deep inside StreamingDataset construction. Checks for each split's `index.json`, the file
    streaming.MDSWriter always writes last -- its presence is what actually means "this split
    was fully exported," not just that the directory exists.
    """
    missing_splits = [split for split in splits if not Path(join_storage_path(base_shards_root, split), "index.json").exists()]
    if missing_splits:
        raise FileNotFoundError(
            f"No shards found for split(s) {missing_splits} under {base_shards_root!r}. Run "
            f"notebooks/31_export_gold_shards.ipynb first for this dataset_version, or pass the "
            f"correct --shards-root if you already exported them somewhere else."
        )


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


@dataclass
class PreparedRun:
    """Everything resolved before training starts: the pinned config, the commit, and where the
    shards are."""

    spec: TrainingRunSpec
    git_commit: str
    random_seed: int
    shards_root: str
    manifest_table: str
    device: str


def prepare_run(
    training_run_name: str, config_root: Path, shards_root: str | None, device: str | None, git_commit: str | None
) -> PreparedRun:
    """Resolve and check everything cheap first, so a bad config or missing shards fail before any
    MLflow setup or data loading."""
    resolved_commit = resolve_git_commit(explicit_commit=git_commit)
    spec = resolve_training_run(config_root, training_run_name)
    check_metadata_preprocessing_is_used(spec.architecture, spec.metadata_preprocessing_version)

    storage_config = load_yaml_config(config_root / "storage.yaml")
    base_shards_root = resolve_shards_root(shards_root, storage_config, spec.dataset_version)
    check_shards_exist(base_shards_root)

    resolved_device = device or ("cuda" if torch.cuda.is_available() else "cpu")
    if device is None and resolved_device == "cpu":
        logger.warning(
            "No CUDA device found, so training will run on CPU. If this should be a GPU run, the "
            "installed torch build probably has no CUDA support (check torch.version.cuda)."
        )

    return PreparedRun(
        spec=spec,
        git_commit=resolved_commit,
        random_seed=spec.preprocessing_config["random_seed"],
        shards_root=base_shards_root,
        manifest_table=storage_config["manifest_table"],
        device=resolved_device,
    )


def build_loaders(run: PreparedRun, num_workers: int) -> dict:
    """One DataLoader per split, with the recipe's train or eval transforms."""
    label_to_index = label_to_index_map(run.spec.label_values)
    train_transform = build_transforms(run.spec.preprocessing_config, "train")
    eval_transform = build_transforms(run.spec.preprocessing_config, "eval")
    split_settings = {"train": (train_transform, True), "validation": (eval_transform, False), "test": (eval_transform, False)}
    return {
        split: build_dataloader(
            join_storage_path(run.shards_root, split),
            transform,
            label_to_index,
            run.spec.batch_size,
            shuffle=shuffle,
            num_workers=num_workers,
            shuffle_seed=run.random_seed,
        )
        for split, (transform, shuffle) in split_settings.items()
    }


def log_run_metadata(run: PreparedRun) -> None:
    """Record the run's inputs, recipes and parameters on the active MLflow run. The full recipes
    are logged, not just their names: a recipe file edited in place keeps its name."""
    log_training_inputs(run.spec.dataset_version, run.shards_root, run.manifest_table)
    mlflow.log_dict(run.spec.preprocessing_config, "preprocessing_config.json")
    if run.spec.metadata_preprocessing_config is not None:
        mlflow.log_dict(run.spec.metadata_preprocessing_config, "metadata_preprocessing_config.json")
    mlflow.log_params(
        {
            "architecture": run.spec.architecture,
            "pretrained": run.spec.pretrained,
            "batch_size": run.spec.batch_size,
            "num_epochs": run.spec.num_epochs,
            "learning_rate": run.spec.learning_rate,
            "optimizer": run.spec.optimizer,
            "random_seed": run.random_seed,
            "git_commit": run.git_commit,
            "device": run.device,
        }
    )


def fit_and_evaluate(model: nn.Module, loaders: dict, run: PreparedRun) -> dict:
    """Train for the configured epochs, logging train and validation metrics each epoch, then
    evaluate on the test split and log its metrics. Returns the test metrics."""
    optimizer = build_optimizer(model, run.spec.optimizer, run.spec.learning_rate)
    loss_fn = nn.CrossEntropyLoss()

    for epoch in range(run.spec.num_epochs):
        train_metrics = train_one_epoch(model, loaders["train"], optimizer, loss_fn, run.device)
        validation_metrics = evaluate(model, loaders["validation"], run.device, run.spec.label_values, loss_fn)
        mlflow.log_metrics(
            {
                "train_loss": train_metrics["train_loss"],
                "validation_loss": validation_metrics["loss"],
                "validation_balanced_accuracy": validation_metrics["balanced_accuracy"],
            },
            step=epoch,
        )

    test_metrics = evaluate(model, loaders["test"], run.device, run.spec.label_values, loss_fn)
    mlflow.log_metrics(
        {
            "test_balanced_accuracy": test_metrics["balanced_accuracy"],
            **{f"test_recall_{label}": recall for label, recall in test_metrics["per_class_recall"].items()},
        }
    )
    mlflow.log_dict(
        {"confusion_matrix": test_metrics["confusion_matrix"], "label_values": run.spec.label_values},
        "confusion_matrix.json",
    )
    mlflow.log_text(test_metrics["classification_report"], "classification_report.txt")
    return test_metrics


def log_and_register_model(model: nn.Module, run: PreparedRun, registered_model_name: str | None) -> None:
    """Log the model with its signature and, if a name is given, register it in the model registry
    with the run's lineage tags. serialization_format="pickle" avoids mlflow's default traced
    format, which needs a real input example; Unity Catalog needs the signature to register."""
    model_info = mlflow.pytorch.log_model(
        model,
        artifact_path="model",
        serialization_format="pickle",
        signature=model_signature(model, run.spec.preprocessing_config["image_size"], run.device),
        registered_model_name=registered_model_name,
    )
    if registered_model_name is not None:
        tag_registered_model_version(
            registered_model_name,
            str(model_info.registered_model_version),
            {
                "training_run_name": run.spec.training_run_name,
                "dataset_version": run.spec.dataset_version,
                "preprocessing_version": run.spec.preprocessing_version,
                "git_commit": run.git_commit,
            },
        )


def run_training(
    training_run_name: str,
    config_root: str | Path | None = None,
    shards_root: str | None = None,
    mlflow_experiment: str | None = None,
    mlflow_tracking_uri: str | None = None,
    registered_model_name: str | None = None,
    registry_uri: str | None = None,
    device: str | None = None,
    num_workers: int = 0,
    git_commit: str | None = None,
) -> str:
    """Train, evaluate and log one pinned training run; returns the MLflow run_id. The same call
    runs from the command line and from notebooks/40_train_baseline_classifier.ipynb.

    `registered_model_name` (falling back to the training-run config's own field) also registers
    the model, e.g. under a Unity Catalog `catalog.schema.model` name. `git_commit` overrides the
    commit lookup, for jobs deployed as a bundle.
    """
    run = prepare_run(
        training_run_name,
        Path(config_root) if config_root is not None else DEFAULT_CONFIG_ROOT,
        shards_root,
        device,
        git_commit,
    )
    torch.manual_seed(run.random_seed)

    configure_mlflow_tracking(mlflow_experiment or run.spec.mlflow_experiment, mlflow_tracking_uri)
    resolved_registered_model_name = registered_model_name or run.spec.registered_model_name
    if resolved_registered_model_name is not None:
        configure_model_registry(registry_uri)

    loaders = build_loaders(run, num_workers)
    model = build_model(run.spec.architecture, len(run.spec.label_values), run.spec.pretrained).to(run.device)

    with mlflow.start_run(
        run_name=training_run_name,
        tags={
            "training_run_name": run.spec.training_run_name,
            "dataset_version": run.spec.dataset_version,
            "preprocessing_version": run.spec.preprocessing_version,
        },
    ) as mlflow_run:
        log_run_metadata(run)
        fit_and_evaluate(model, loaders, run)
        log_and_register_model(model, run, resolved_registered_model_name)
        return mlflow_run.info.run_id


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    args = parse_args(argv)
    run_id = run_training(
        training_run_name=args.training_run_name,
        config_root=args.config_root,
        shards_root=args.shards_root,
        mlflow_experiment=args.mlflow_experiment,
        mlflow_tracking_uri=args.mlflow_tracking_uri,
        registered_model_name=args.registered_model_name,
        registry_uri=args.registry_uri,
        device=args.device,
        num_workers=args.num_workers,
    )
    print(f"MLflow run_id: {run_id}")


if __name__ == "__main__":
    main()
