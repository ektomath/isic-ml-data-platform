"""Training-run config loading and resolution.

Pure Python, no Spark dependency -- unit-tested locally (see tests/test_ml_training_run.py).

`config/gold/training_runs/<name>.yaml` pins exactly one dataset_version to exactly one
preprocessing_version (see docs/decisions/010-pin-data-and-preprocessing-per-training-run.md).
`resolve_training_run` is the one place a `training_run_name` turns into everything downstream
training code needs -- the enforcement point for "the entrypoint accepts only this one name,"
never free-standing dataset_version/preprocessing_version parameters.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from data_platform.dataset_layout import load_yaml_config
from ml.metadata_preprocessing import load_metadata_preprocessing_config
from ml.preprocessing import load_preprocessing_config, require_fields

TRAINING_RUN_REQUIRED_FIELDS = (
    "training_run_name",
    "dataset_version",
    "preprocessing_version",
    "label_values",
    "architecture",
    "batch_size",
    "num_epochs",
    "learning_rate",
)


@dataclass(frozen=True)
class TrainingRunSpec:
    training_run_name: str
    dataset_version: str
    preprocessing_version: str
    preprocessing_config: dict
    label_values: list[str]
    architecture: str
    pretrained: bool
    batch_size: int
    num_epochs: int
    learning_rate: float
    optimizer: str
    mlflow_experiment: str | None
    registered_model_name: str | None
    metadata_preprocessing_version: str | None
    metadata_preprocessing_config: dict | None


def load_training_run_config(config_root: str | Path, training_run_name: str) -> dict:
    """Load config/gold/training_runs/<training_run_name>.yaml and validate it carries every
    field TrainingRunSpec needs."""
    path = Path(config_root) / "gold" / "training_runs" / f"{training_run_name}.yaml"
    config = load_yaml_config(path)
    require_fields(config, TRAINING_RUN_REQUIRED_FIELDS, f"Training-run config {path}")
    return config


def resolve_training_run(config_root: str | Path, training_run_name: str) -> TrainingRunSpec:
    """Load and validate training_run_name's config, then load and validate its referenced
    preprocessing config -- the single call that turns a name into a fully resolved,
    ready-to-train-with spec. Raises ValueError on any missing/malformed field in either config.

    `metadata_preprocessing_version` is optional -- most training runs are image-only, so most
    configs omit it. When set, its referenced config/metadata_preprocessing/<name>.yaml is
    loaded and validated the same way preprocessing_version's is, so a multimodal (image +
    tabular/text) model can build its metadata feature vector
    (ml.metadata_preprocessing.build_metadata_transform) from a pinned, versioned recipe rather
    than an ad hoc local column selection -- see
    docs/decisions/012-pin-metadata-feature-config-per-training-run.md.
    """
    config = load_training_run_config(config_root, training_run_name)
    preprocessing_config = load_preprocessing_config(config_root, config["preprocessing_version"])
    metadata_preprocessing_version = config.get("metadata_preprocessing_version")
    metadata_preprocessing_config = (
        load_metadata_preprocessing_config(config_root, metadata_preprocessing_version)
        if metadata_preprocessing_version is not None
        else None
    )

    return TrainingRunSpec(
        training_run_name=config["training_run_name"],
        dataset_version=config["dataset_version"],
        preprocessing_version=config["preprocessing_version"],
        preprocessing_config=preprocessing_config,
        label_values=list(config["label_values"]),
        architecture=config["architecture"],
        pretrained=bool(config.get("pretrained", True)),
        batch_size=int(config["batch_size"]),
        num_epochs=int(config["num_epochs"]),
        learning_rate=float(config["learning_rate"]),
        optimizer=config.get("optimizer", "adam"),
        mlflow_experiment=config.get("mlflow_experiment"),
        registered_model_name=config.get("registered_model_name"),
        metadata_preprocessing_version=metadata_preprocessing_version,
        metadata_preprocessing_config=metadata_preprocessing_config,
    )
